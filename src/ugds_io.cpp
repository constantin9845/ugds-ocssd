#include "ugds_internal.h"

#include <cstring>
#include <cstdio>
#include <cerrno>
#include <atomic>
#include <algorithm>
#include <poll.h>
#include <unistd.h>
#include <time.h>

static nvm_cpl_t* wait_for_completion(HandleState* hs, IOQueuePair& qp)
{


    fprintf(stderr,
        "uGDS wait: irq_efd=%d timeout=%ld\n",
        qp.irq_efd, (long)hs->ctrl->timeout);

    if (qp.irq_efd < 0) {
        nvm_cpl_t* cpl = nullptr;
        uint64_t spins = 0;
        const uint64_t max_spins = (uint64_t)hs->ctrl->timeout * 1000000ULL;
        while ((cpl = nvm_cq_dequeue(&qp.cq)) == nullptr) {
            if (++spins > max_spins) break;
            __builtin_ia32_pause();
        }

	if (cpl == nullptr) {
        	fprintf(stderr,
                	"uGDS timeout: CQ empty after %llu spins\n",
                	(unsigned long long)spins);
    	} else {
        	fprintf(stderr,
                	"uGDS completion received: cid=%u\n",
                	cpl->dword[0] & 0xffff);
    	}

        	return cpl;
    	}

    // Absolute deadline prevents EINTR restarts from stretching total wait.
    const long timeout_ms = (long)hs->ctrl->timeout;
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += timeout_ms / 1000;
    deadline.tv_nsec += (timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        deadline.tv_nsec -= 1000000000L;
    }

    for (;;) {
        // Poll CQ before blocking to prevent lost wakeups.
        nvm_cpl_t* cpl = nvm_cq_dequeue(&qp.cq);
        if (cpl != nullptr) return cpl;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        struct timespec ts;
        ts.tv_sec = deadline.tv_sec - now.tv_sec;
        ts.tv_nsec = deadline.tv_nsec - now.tv_nsec;
        if (ts.tv_nsec < 0) {
            ts.tv_sec -= 1;
            ts.tv_nsec += 1000000000L;
        }
        if (ts.tv_sec < 0)
            return nvm_cq_dequeue(&qp.cq);

        struct pollfd pfd = {qp.irq_efd, POLLIN, 0};
        int pr = ppoll(&pfd, 1, &ts, nullptr);
        if (pr < 0) {
            if (errno == EINTR) continue;
            fprintf(stderr, "uGDS: ppoll(eventfd=%d) failed: %s\n",
                    qp.irq_efd, strerror(errno));
            return nullptr;
        }
        if (pr == 0){
	    nvm_cpl_t* cpl = nvm_cq_dequeue(&qp.cq);

	    fprintf(stderr,
            	"uGDS ppoll timeout: eventfd=%d, CQ completion=%s\n",
            	qp.irq_efd,
            	cpl != nullptr ? "present" : "absent");

    	    return cpl;

	   //return nvm_cq_dequeue(&qp.cq);
	}

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            fprintf(stderr, "uGDS: eventfd=%d invalid, revents=0x%x\n",
                    qp.irq_efd, pfd.revents);
            return nullptr;
        }
        if (!(pfd.revents & POLLIN))
            continue;

        uint64_t cnt;
        ssize_t r = read(qp.irq_efd, &cnt, sizeof(cnt));
        if (r == (ssize_t)sizeof(cnt))
            continue;
        if (r < 0 && (errno == EINTR || errno == EAGAIN))
            continue;
        fprintf(stderr, "uGDS: read(eventfd=%d) failed: %s\n",
                qp.irq_efd, r < 0 ? strerror(errno) : "short read");
        return nullptr;
    }
}

ssize_t do_io_internal(uGDSHandle_t fh, void* bufPtr_base, size_t size,
                       uint64_t file_offset, off_t bufPtr_offset, uint8_t opcode)
{
    HandleState* hs = static_cast<HandleState*>(fh);

    if (hs == nullptr || bufPtr_base == nullptr || size == 0) {
        return -EINVAL;
    }
    if (bufPtr_offset < 0) {
        return -EINVAL;
    }
    if (hs->block_size == 0) {
        return -EINVAL;
    }
    /*
    if ((size % hs->block_size) != 0) {
        return -EINVAL;
    }
    */

    const size_t page_size = hs->ctrl->page_size;
    if (page_size == 0) {
        return -EINVAL;
    }

    nvm_dma_t* buf_dma = nullptr;
    bool on_the_fly = false;
    size_t buf_page_start = 0;

    {
        std::lock_guard<std::mutex> drv_lock(g_driver.lock);
        auto it = g_driver.buf_registry.find(bufPtr_base);
        if (it != g_driver.buf_registry.end()) {
            buf_dma = it->second.dma;
            it->second.in_flight.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    if (buf_dma != nullptr) {
        if ((static_cast<size_t>(bufPtr_offset) % page_size) != 0) {
            std::lock_guard<std::mutex> drv_lock(g_driver.lock);
            auto it = g_driver.buf_registry.find(bufPtr_base);
            if (it != g_driver.buf_registry.end())
                it->second.in_flight.fetch_sub(1, std::memory_order_acq_rel);
            return -EINVAL;
        }
        buf_page_start = static_cast<size_t>(bufPtr_offset) / page_size;
        /* Overflow-safe bounds check: compute pages_needed separately,
         * then verify buf_page_start + pages_needed <= n_ioaddrs
         * without risking wrap. */
        size_t pages_needed = (size - 1) / page_size + 1;
        if (pages_needed > buf_dma->n_ioaddrs ||
            buf_page_start > buf_dma->n_ioaddrs - pages_needed) {
            std::lock_guard<std::mutex> drv_lock(g_driver.lock);
            auto it = g_driver.buf_registry.find(bufPtr_base);
            if (it != g_driver.buf_registry.end())
                it->second.in_flight.fetch_sub(1, std::memory_order_acq_rel);
            return -EINVAL;
        }
    } else {
        void* map_ptr = static_cast<uint8_t*>(bufPtr_base) + bufPtr_offset;
        // Reject non-64KB-aligned on-the-fly buffers (kernel driver silently rounds down)
        if ((reinterpret_cast<uintptr_t>(map_ptr) & ((1UL << 16) - 1)) != 0) {
            return -EINVAL;
        }
        int rc = nvm_dma_map_device(&buf_dma, hs->ctrl, map_ptr, size);
        if (rc != 0 || buf_dma == nullptr) {
            return -EIO;
        }
        on_the_fly = true;
        buf_page_start = 0;
    }

    //const uint64_t start_lba = static_cast<uint64_t>(file_offset) / hs->block_size;
    const uint64_t start_lba = static_cast<uint64_t>(file_offset);

    const size_t prp_capacity = page_size / sizeof(uint64_t);

    size_t max_xfer = hs->max_transfer_size;
    if (max_xfer == 0) {
        max_xfer = UGDS_DEFAULT_MAX_TRANSFER_SIZE;
    }
    if (max_xfer < page_size) {
        max_xfer = page_size;
    }
    // Cap by PRP list capacity: PRP1 + prp_capacity entries → (prp_capacity + 1) pages
    size_t prp_max = (prp_capacity + 1) * page_size;
    if (max_xfer > prp_max) {
        max_xfer = prp_max;
    }

    const uint16_t qp_idx = static_cast<uint16_t>(
        hs->rr_counter.fetch_add(1) % hs->num_qps);
    IOQueuePair& qp = *hs->qps[qp_idx];

    ssize_t result = 0;
    bool timed_out = false;
    size_t bytes_done = 0;
    uint64_t current_lba = start_lba;
    size_t current_page = buf_page_start;

    {
        std::lock_guard<std::mutex> qp_lock(qp.lock);

        /* Re-check wedged after acquiring QP lock: a previous
         * operation may have timed out while we waited. */
        if (hs->wedged.load(std::memory_order_acquire)) {
            result = -EBADF;
            goto out;
        }

        while (bytes_done < size) {
            size_t remaining = size - bytes_done;
            size_t chunk_size = std::min(remaining, max_xfer);

            chunk_size = (chunk_size / hs->block_size) * hs->block_size;
            if (chunk_size == 0) {
                result = -EINVAL;
                goto out;
            }

            size_t n_blocks = chunk_size / hs->block_size;
            size_t n_pages = (chunk_size + page_size - 1) / page_size;
            if (n_pages == 0) n_pages = 1;

            nvm_cmd_t* cmd = nullptr;
            while ((cmd = nvm_sq_enqueue(&qp.sq)) == nullptr) {
                nvm_cpl_t* drain = wait_for_completion(hs, qp);
                if (drain == nullptr) {
                    result = -EIO;
                    /* An older submitted command may still DMA. Treat this
                     * exactly like a post-submit completion timeout. */
                    timed_out = true;
                    hs->wedged.store(true, std::memory_order_release);
                    goto out;
                }
                uint16_t st = UGDS_CPL_SCT_SC(drain);
                nvm_sq_update(&qp.sq);
                std::atomic_thread_fence(std::memory_order_seq_cst);
                nvm_cq_update(&qp.cq);
                if (st != 0) {
                    result = -EIO;
                    goto out;
                }
            }

            memset(cmd, 0, sizeof(nvm_cmd_t));
            uint16_t cid = NVM_DEFAULT_CID(&qp.sq);
            nvm_cmd_header(cmd, cid, opcode, hs->ns_id);

		fprintf(stderr,
    			"DMA debug: \n\tbuf_dma=%p \n\tcurrent_page=%zu "
    			"\nt\n_ioaddrs=%zu \n\tioaddr=%llx\n",
    			(void*)buf_dma,
    			current_page,
    			buf_dma->n_ioaddrs,
    			(unsigned long long)buf_dma->ioaddrs[current_page]);

            if (n_pages == 1) {
                nvm_cmd_data_ptr(cmd, buf_dma->ioaddrs[current_page], 0);
            } else if (n_pages == 2) {
                nvm_cmd_data_ptr(cmd,
                                 buf_dma->ioaddrs[current_page],
                                 buf_dma->ioaddrs[current_page + 1]);
            } else {
                volatile uint64_t* prp_list =
                    reinterpret_cast<volatile uint64_t*>(qp.prp_dma->vaddr);
                for (size_t i = 1; i < n_pages; ++i) {
                    prp_list[i - 1] = buf_dma->ioaddrs[current_page + i];
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                nvm_cmd_data_ptr(cmd,
                                 buf_dma->ioaddrs[current_page],
                                 qp.prp_dma->ioaddrs[0]);
            }

	    printf("current LBA = %llu\n", (unsigned long long)current_lba);

            nvm_cmd_rw_blks(cmd, current_lba, static_cast<uint16_t>(n_blocks));

            std::atomic_thread_fence(std::memory_order_seq_cst);

	    fprintf(stderr,
        "uGDS submit: opcode=0x%02x nsid=%u "
        "PPA=0x%016llx blocks=%zu chunk=%zu "
        "page_size=%zu n_pages=%zu "
        "\n\tPRP1=0x%016llx \n\tPRP2=0x%016llx\n",
        opcode,
        hs->ns_id,
        (unsigned long long)current_lba,
        n_blocks,
        chunk_size,
        page_size,
        n_pages,
        (unsigned long long)buf_dma->ioaddrs[current_page],
        (unsigned long long)(n_pages == 2
            ? buf_dma->ioaddrs[current_page + 1]
            : n_pages > 2 ? qp.prp_dma->ioaddrs[0] : 0));


	    fprintf(stderr,
    "\n\tCMD: opcode=%02x nsid=%u "
    "\nt\DW10=%08x DW11=%08x DW12=%08x "
    "\n\tPRP1=%08x%08x PRP2=%08x%08x\n",
    cmd->dword[0] & 0xff,
    cmd->dword[1],
    cmd->dword[10], cmd->dword[11], cmd->dword[12],
    cmd->dword[7], cmd->dword[6],
    cmd->dword[9], cmd->dword[8]);

            nvm_sq_submit(&qp.sq);
            std::atomic_thread_fence(std::memory_order_seq_cst);

            nvm_cpl_t* cpl = wait_for_completion(hs, qp);
            if (cpl == nullptr) {
                result = -EIO;
                timed_out = true;
                /* Mark wedged while still holding qp.lock to prevent
                 * QP reuse before the flag is visible. */
                hs->wedged.store(true, std::memory_order_release);
                goto out;
            }

            uint16_t status = UGDS_CPL_SCT_SC(cpl);

            nvm_sq_update(&qp.sq);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            nvm_cq_update(&qp.cq);
	    /*
            if (status != 0) {
                result = -EIO;
                goto out;
            }
	    */

	    if (status != 0) {
 		fprintf(stderr,
            	"uGDS NVMe completion error: "
            	"dword3=0x%08x SCT_SC=0x%03x "
            	"PPA=0x%016llx blocks=%zu opcode=0x%02x\n",
            	cpl->dword[3],
            	(unsigned)status,
            	(unsigned long long)current_lba,
            	n_blocks,
            	opcode
		);
    		result = -EIO;
    		goto out;
	    }

            bytes_done += chunk_size;
            current_lba += n_blocks;
            current_page += n_pages;
        }

        result = static_cast<ssize_t>(bytes_done);

        /* Transfer timeout resources to the QP while we still hold
         * qp.lock.  A concurrent force teardown also acquires qp.lock
         * in cleanup_timeout_resources(), so this prevents the race
         * where teardown destroys the QP before we store the mapping. */
        if (timed_out) {
            if (on_the_fly) {
                qp.timeout_dma = buf_dma;
            } else {
                qp.timeout_registered_buf = bufPtr_base;
            }
            buf_dma = nullptr;
        }

    out:;
    }

    if (timed_out) {
        /* NVMe command may still be executing. wedged was set
         * under qp.lock. Resources were transferred to the QP above. */
        fprintf(stderr, "uGDS: I/O timeout -- handle wedged. "
                "Controller reset required.\n");
    } else if (on_the_fly && buf_dma != nullptr) {
        nvm_dma_unmap(buf_dma);
    } else if (!on_the_fly && buf_dma != nullptr) {
        /* Registered buffer: release in-flight reference. */
        std::lock_guard<std::mutex> drv_lock(g_driver.lock);
        auto it = g_driver.buf_registry.find(bufPtr_base);
        if (it != g_driver.buf_registry.end())
            it->second.in_flight.fetch_sub(1, std::memory_order_acq_rel);
    }

    return result;
}

extern "C" ssize_t uGDSRead(uGDSHandle_t fh, void* bufPtr_base, size_t size,
                              uint64_t file_offset, off_t bufPtr_offset)
{
    if (fh == nullptr) return -EINVAL;
    std::shared_ptr<HandleState> hs_sp;
    HandleState* hs = handle_lookup(fh, &hs_sp);
    if (!hs) return -EBADF;
    ssize_t ret = do_io_internal(fh, bufPtr_base, size, file_offset, bufPtr_offset, NVM_IO_READ);
    handle_release(hs);
    return ret;
}

extern "C" ssize_t uGDSWrite(uGDSHandle_t fh, const void* bufPtr_base, size_t size,
                               uint64_t file_offset, off_t bufPtr_offset)
{
    if (fh == nullptr) return -EINVAL;
    std::shared_ptr<HandleState> hs_sp;
    HandleState* hs = handle_lookup(fh, &hs_sp);
    if (!hs) return -EBADF;
    ssize_t ret = do_io_internal(fh, const_cast<void*>(bufPtr_base), size, file_offset, bufPtr_offset,
                 NVM_IO_WRITE);
    handle_release(hs);
    return ret;
}

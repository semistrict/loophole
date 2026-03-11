//go:build darwin
// +build darwin

#include "_cgo_export.h"
#include "mach_exc.h"

#include <mach/mach.h>
#include <mach/mach_error.h>
#include <mach/mach_param.h>
#include <mach/mach_vm.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>

#define MMAP_DARWIN_STOP_MSG_ID 0x4d4d4150
#define MMAP_DARWIN_SERVER_THREADS 1

typedef struct {
	mach_msg_header_t Head;
} mmap_darwin_stop_msg_t;

typedef union {
	mach_msg_header_t Head;
	union __RequestUnion__mach_exc_subsystem Request;
	mmap_darwin_stop_msg_t Stop;
	uint8_t Raw[4096];
} mmap_darwin_request_t;

typedef union {
	mach_msg_header_t Head;
	union __ReplyUnion__mach_exc_subsystem Reply;
	uint8_t Raw[4096];
} mmap_darwin_reply_t;

extern boolean_t mach_exc_server(mach_msg_header_t *InHeadP, mach_msg_header_t *OutHeadP);

static struct {
	mach_port_t exception_port;
	pthread_t threads[MMAP_DARWIN_SERVER_THREADS];
	int installed;
	exception_mask_t old_masks[TASK_MAX_EXCEPTION_PORT_COUNT];
	mach_port_t old_ports[TASK_MAX_EXCEPTION_PORT_COUNT];
	exception_behavior_t old_behaviors[TASK_MAX_EXCEPTION_PORT_COUNT];
	thread_state_flavor_t old_flavors[TASK_MAX_EXCEPTION_PORT_COUNT];
	mach_msg_type_number_t old_count;
} g_mmap_darwin;

static void *mmap_darwin_server_main(void *arg) {
	mach_port_t port = (mach_port_t)(uintptr_t)arg;

	for (;;) {
		mmap_darwin_request_t request;
		kern_return_t kr = mach_msg(
			&request.Head,
			MACH_RCV_MSG,
			0,
			sizeof(request),
			port,
			MACH_MSG_TIMEOUT_NONE,
			MACH_PORT_NULL
		);
		if (kr == MACH_RCV_INTERRUPTED) {
			continue;
		}
		if (kr != MACH_MSG_SUCCESS) {
			break;
		}

		if (request.Head.msgh_id == MMAP_DARWIN_STOP_MSG_ID) {
			break;
		}

		mmap_darwin_reply_t reply;
		memset(&reply, 0, sizeof(reply));
		if (!mach_exc_server(&request.Head, &reply.Head)) {
			continue;
		}
		if (reply.Head.msgh_remote_port != MACH_PORT_NULL) {
			(void)mach_msg(
				&reply.Head,
				MACH_SEND_MSG,
				reply.Head.msgh_size,
				0,
				MACH_PORT_NULL,
				MACH_MSG_TIMEOUT_NONE,
				MACH_PORT_NULL
			);
		}
	}

	return NULL;
}

static kern_return_t mmap_darwin_restore_exception_ports(void) {
	mach_msg_type_number_t i;
	int restored = 0;

	for (i = 0; i < g_mmap_darwin.old_count; i++) {
		exception_mask_t mask = g_mmap_darwin.old_masks[i] & EXC_MASK_BAD_ACCESS;
		if (mask == 0) {
			continue;
		}
		kern_return_t kr = task_set_exception_ports(
			mach_task_self(),
			mask,
			g_mmap_darwin.old_ports[i],
			g_mmap_darwin.old_behaviors[i],
			g_mmap_darwin.old_flavors[i]
		);
		if (kr != KERN_SUCCESS) {
			return kr;
		}
		restored = 1;
	}

	if (!restored) {
		return task_set_exception_ports(
			mach_task_self(),
			EXC_MASK_BAD_ACCESS,
			MACH_PORT_NULL,
			EXCEPTION_DEFAULT,
			THREAD_STATE_NONE
		);
	}

	return KERN_SUCCESS;
}

static kern_return_t mmap_darwin_set_current_thread_ports(mach_port_t port) {
	thread_act_array_t threads = NULL;
	mach_msg_type_number_t thread_count = 0;

	kern_return_t kr = task_threads(mach_task_self(), &threads, &thread_count);
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	for (mach_msg_type_number_t i = 0; i < thread_count; i++) {
		kr = thread_set_exception_ports(
			threads[i],
			EXC_MASK_BAD_ACCESS,
			port,
			EXCEPTION_DEFAULT | MACH_EXCEPTION_CODES,
			THREAD_STATE_NONE
		);
		mach_port_deallocate(mach_task_self(), threads[i]);
		if (kr != KERN_SUCCESS) {
			break;
		}
	}

	vm_deallocate(
		mach_task_self(),
		(vm_address_t)threads,
		(vm_size_t)(thread_count * sizeof(thread_t))
	);
	return kr;
}

kern_return_t mmap_darwin_install_exception_handler(void) {
	if (g_mmap_darwin.installed) {
		return KERN_SUCCESS;
	}

	memset(&g_mmap_darwin, 0, sizeof(g_mmap_darwin));

	kern_return_t kr = mach_port_allocate(
		mach_task_self(),
		MACH_PORT_RIGHT_RECEIVE,
		&g_mmap_darwin.exception_port
	);
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	kr = mach_port_insert_right(
		mach_task_self(),
		g_mmap_darwin.exception_port,
		g_mmap_darwin.exception_port,
		MACH_MSG_TYPE_MAKE_SEND
	);
	if (kr != KERN_SUCCESS) {
		mach_port_destroy(mach_task_self(), g_mmap_darwin.exception_port);
		g_mmap_darwin.exception_port = MACH_PORT_NULL;
		return kr;
	}

	mach_port_limits_t limits;
	limits.mpl_qlimit = MACH_PORT_QLIMIT_LARGE;
	kr = mach_port_set_attributes(
		mach_task_self(),
		g_mmap_darwin.exception_port,
		MACH_PORT_LIMITS_INFO,
		(mach_port_info_t)&limits,
		MACH_PORT_LIMITS_INFO_COUNT
	);
	if (kr != KERN_SUCCESS) {
		mach_port_destroy(mach_task_self(), g_mmap_darwin.exception_port);
		g_mmap_darwin.exception_port = MACH_PORT_NULL;
		return kr;
	}

	g_mmap_darwin.old_count = TASK_MAX_EXCEPTION_PORT_COUNT;
	kr = task_swap_exception_ports(
		mach_task_self(),
		EXC_MASK_BAD_ACCESS,
		g_mmap_darwin.exception_port,
		EXCEPTION_DEFAULT | MACH_EXCEPTION_CODES,
		THREAD_STATE_NONE,
		g_mmap_darwin.old_masks,
		&g_mmap_darwin.old_count,
		g_mmap_darwin.old_ports,
		g_mmap_darwin.old_behaviors,
		g_mmap_darwin.old_flavors
	);
	if (kr != KERN_SUCCESS) {
		mach_port_destroy(mach_task_self(), g_mmap_darwin.exception_port);
		g_mmap_darwin.exception_port = MACH_PORT_NULL;
		return kr;
	}

	kr = mmap_darwin_set_current_thread_ports(g_mmap_darwin.exception_port);
	if (kr != KERN_SUCCESS) {
		kern_return_t restore_kr = mmap_darwin_restore_exception_ports();
		(void)restore_kr;
		mach_port_destroy(mach_task_self(), g_mmap_darwin.exception_port);
		g_mmap_darwin.exception_port = MACH_PORT_NULL;
		return kr;
	}

	for (int i = 0; i < MMAP_DARWIN_SERVER_THREADS; i++) {
		if (pthread_create(&g_mmap_darwin.threads[i], NULL, mmap_darwin_server_main,
			(void *)(uintptr_t)g_mmap_darwin.exception_port) != 0) {
			kern_return_t restore_kr = mmap_darwin_restore_exception_ports();
			(void)restore_kr;
			for (int j = 0; j < i; j++) {
				mmap_darwin_stop_msg_t stop_msg;
				memset(&stop_msg, 0, sizeof(stop_msg));
				stop_msg.Head.msgh_bits = MACH_MSGH_BITS(MACH_MSG_TYPE_COPY_SEND, 0);
				stop_msg.Head.msgh_size = sizeof(stop_msg);
				stop_msg.Head.msgh_remote_port = g_mmap_darwin.exception_port;
				stop_msg.Head.msgh_local_port = MACH_PORT_NULL;
				stop_msg.Head.msgh_id = MMAP_DARWIN_STOP_MSG_ID;
				(void)mach_msg(
					&stop_msg.Head,
					MACH_SEND_MSG,
					stop_msg.Head.msgh_size,
					0,
					MACH_PORT_NULL,
					MACH_MSG_TIMEOUT_NONE,
					MACH_PORT_NULL
				);
				(void)pthread_join(g_mmap_darwin.threads[j], NULL);
			}
			mach_port_destroy(mach_task_self(), g_mmap_darwin.exception_port);
			g_mmap_darwin.exception_port = MACH_PORT_NULL;
			return KERN_FAILURE;
		}
	}

	g_mmap_darwin.installed = 1;
	return KERN_SUCCESS;
}

kern_return_t mmap_darwin_uninstall_exception_handler(void) {
	if (!g_mmap_darwin.installed) {
		return KERN_SUCCESS;
	}

	kern_return_t kr = mmap_darwin_restore_exception_ports();
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	kr = mmap_darwin_set_current_thread_ports(MACH_PORT_NULL);
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	for (int i = 0; i < MMAP_DARWIN_SERVER_THREADS; i++) {
		mmap_darwin_stop_msg_t thread_stop_msg;
		memset(&thread_stop_msg, 0, sizeof(thread_stop_msg));
		thread_stop_msg.Head.msgh_bits = MACH_MSGH_BITS(MACH_MSG_TYPE_COPY_SEND, 0);
		thread_stop_msg.Head.msgh_size = sizeof(thread_stop_msg);
		thread_stop_msg.Head.msgh_remote_port = g_mmap_darwin.exception_port;
		thread_stop_msg.Head.msgh_local_port = MACH_PORT_NULL;
		thread_stop_msg.Head.msgh_id = MMAP_DARWIN_STOP_MSG_ID;

		kr = mach_msg(
			&thread_stop_msg.Head,
			MACH_SEND_MSG,
			thread_stop_msg.Head.msgh_size,
			0,
			MACH_PORT_NULL,
			MACH_MSG_TIMEOUT_NONE,
			MACH_PORT_NULL
		);
		if (kr != KERN_SUCCESS) {
			return kr;
		}
	}

	for (int i = 0; i < MMAP_DARWIN_SERVER_THREADS; i++) {
		if (pthread_join(g_mmap_darwin.threads[i], NULL) != 0) {
			return KERN_FAILURE;
		}
	}

	kr = mach_port_destroy(mach_task_self(), g_mmap_darwin.exception_port);
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	memset(&g_mmap_darwin, 0, sizeof(g_mmap_darwin));
	return KERN_SUCCESS;
}

kern_return_t mmap_darwin_resolve_page(uint64_t page_addr, const void *src, uintptr_t len) {
	kern_return_t kr = mach_vm_protect(
		mach_task_self(),
		(mach_vm_address_t)page_addr,
		(mach_vm_size_t)len,
		FALSE,
		VM_PROT_READ | VM_PROT_WRITE
	);
	if (kr != KERN_SUCCESS) {
		return kr;
	}

	memcpy((void *)(uintptr_t)page_addr, src, (size_t)len);
	return KERN_SUCCESS;
}

kern_return_t catch_mach_exception_raise(
	mach_port_t exception_port,
	mach_port_t thread,
	mach_port_t task,
	exception_type_t exception,
	mach_exception_data_t code,
	mach_msg_type_number_t codeCnt
) {
	(void)exception_port;
	(void)thread;
	(void)task;

	if (exception != EXC_BAD_ACCESS || codeCnt < 2) {
		return KERN_FAILURE;
	}

	return goMmapDarwinHandleFault((uint64_t)code[1], (int64_t)code[0]);
}

kern_return_t catch_mach_exception_raise_state(
	mach_port_t exception_port,
	exception_type_t exception,
	const mach_exception_data_t code,
	mach_msg_type_number_t codeCnt,
	int *flavor,
	const thread_state_t old_state,
	mach_msg_type_number_t old_stateCnt,
	thread_state_t new_state,
	mach_msg_type_number_t *new_stateCnt
) {
	(void)exception_port;
	(void)exception;
	(void)code;
	(void)codeCnt;
	(void)flavor;
	(void)old_state;
	(void)old_stateCnt;
	(void)new_state;
	(void)new_stateCnt;
	return KERN_FAILURE;
}

kern_return_t catch_mach_exception_raise_state_identity(
	mach_port_t exception_port,
	mach_port_t thread,
	mach_port_t task,
	exception_type_t exception,
	mach_exception_data_t code,
	mach_msg_type_number_t codeCnt,
	int *flavor,
	thread_state_t old_state,
	mach_msg_type_number_t old_stateCnt,
	thread_state_t new_state,
	mach_msg_type_number_t *new_stateCnt
) {
	(void)exception_port;
	(void)thread;
	(void)task;
	(void)exception;
	(void)code;
	(void)codeCnt;
	(void)flavor;
	(void)old_state;
	(void)old_stateCnt;
	(void)new_state;
	(void)new_stateCnt;
	return KERN_FAILURE;
}

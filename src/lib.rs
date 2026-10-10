pub mod config;
pub mod dispatcher;
pub mod payload;
pub mod risk;
pub mod stream;
pub mod telegram;
pub mod types;
pub mod wallet;

#[repr(C)]
pub struct Timespec64 {
    pub tv_sec: i64,
    pub tv_nsec: i64,
}

/// Compatibility symbol for MinGW UCRT pthread_time.h linkage in aws-lc-sys
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nanosleep64(
    req: *const Timespec64,
    rem: *mut Timespec64,
) -> std::ffi::c_int {
    if req.is_null() {
        return -1;
    }
    let req_val = unsafe { &*req };
    if req_val.tv_sec < 0 || req_val.tv_nsec < 0 || req_val.tv_nsec >= 1_000_000_000 {
        return -1;
    }
    std::thread::sleep(std::time::Duration::new(
        req_val.tv_sec as u64,
        req_val.tv_nsec as u32,
    ));
    if !rem.is_null() {
        unsafe {
            (*rem).tv_sec = 0;
            (*rem).tv_nsec = 0;
        }
    }
    0
}

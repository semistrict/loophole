use std::sync::atomic::{AtomicBool, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(cfg!(test));

pub fn init() {
    ENABLED.store(
        std::env::var_os("LOOPHOLE_ASSERT").is_some(),
        Ordering::Relaxed,
    );
}

#[macro_export]
macro_rules! loophole_assert {
    ($cond:expr, $($arg:tt)+) => {
        if $crate::assert::is_enabled() && !($cond) {
            panic!("LOOPHOLE_ASSERT: {}", format!($($arg)+));
        }
    };
}

pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

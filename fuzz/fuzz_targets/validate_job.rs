#![no_main]

use libfuzzer_sys::fuzz_target;
use nomad_rs::jobspec::Job;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Ok(job) = serde_json::from_str::<Job>(s) {
            let _ = job.validate();
        }
    }
});

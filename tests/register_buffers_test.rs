use async_bpm::bpm::BufferPoolManager;
use async_bpm::page::PageId;
use std::fs::File;
use std::sync::Mutex;
use std::thread;
use std::{ops::DerefMut, sync::Arc};
use tokio::task::LocalSet;
use tracing::{info, trace, Level};

#[test]
fn test_register() {
    let log_file = File::create("test_register.log").unwrap();

    let stdout_subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .without_time()
        .with_max_level(Level::TRACE)
        .with_writer(Mutex::new(log_file))
        .finish();
    tracing::subscriber::set_global_default(stdout_subscriber).unwrap();

    trace!("Starting test_register");

    let bpm = Arc::new(BufferPoolManager::new(4, 4));
    let rt = bpm.build_thread_runtime();

    // Spawn the eviction thread / single task
    let bpm_evictor = bpm.clone();
    bpm_evictor.spawn_evictor();

    let dm = bpm.get_disk_manager();
    let dmh = dm.create_handle();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let (tx, rx) = bpm.free_frames();

        trace!("Getting free frame");

        let mut frame = rx.recv().await.unwrap();
        frame.deref_mut().fill(b'A');

        trace!("Filled frame");

        let pid = PageId::new(0);
        let frame = dmh.write_from_fixed(pid, frame).await.unwrap();

        trace!("Wrote frame data out");

        tx.send(frame).await.unwrap();

        loop {
            tokio::task::yield_now().await;
        }
    });

    rt.block_on(local);
}

#[test]
fn test_register_threads() {
    trace!("Starting test_register_threads");

    let log_file = File::create("test_register_threads.log").unwrap();

    let stdout_subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .without_time()
        .with_max_level(Level::TRACE)
        .with_writer(Mutex::new(log_file))
        .finish();
    tracing::subscriber::set_global_default(stdout_subscriber).unwrap();

    const THREADS: usize = 1;

    let bpm = Arc::new(BufferPoolManager::new(4, 4));
    let dm = bpm.get_disk_manager();

    thread::scope(|s| {
        for i in 0..THREADS {
            let bpm_clone = bpm.clone();
            let dm_clone = dm.clone();

            s.spawn(move || {
                let rt = bpm_clone.build_thread_runtime();

                let dmh = dm_clone.create_handle();
                let uring = dmh.get_uring();

                info!("Registering buffers");

                uring.register_buffers(dm_clone.register_buffers);

                info!("Finished registering buffers");

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let (tx, rx) = bpm_clone.free_frames.clone();

                    trace!("Getting free frame");

                    let mut frame = rx.recv().await.unwrap();
                    frame.deref_mut().fill(b'A' + i as u8);

                    trace!("Filled frame");

                    let pid = PageId::new(i as u64);
                    let frame = dmh.write_from_fixed(pid, frame).await.unwrap();

                    trace!("Wrote frame data out");

                    tx.send(frame).await.unwrap();
                });

                rt.block_on(local);
            });
        }
    });
}

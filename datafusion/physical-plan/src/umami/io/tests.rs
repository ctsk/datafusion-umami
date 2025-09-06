use datafusion_common::{record_batch, Result};
use futures::StreamExt;
use tempfile::NamedTempFile;

use crate::{
    joins::test_utils::compare_batches,
    umami::io::{
        uring::{ReadOpts, RuntimeFreeWriter, WriteOpts},
        AsyncBatchWriter,
    },
};

#[tokio::test]
async fn test_uring_writer() -> Result<()> {
    let tmppath = NamedTempFile::new()?;
    let data = record_batch!(
        ("nums", Int64, vec![42; 42]),
        ("name", Utf8, vec!["foo"; 42])
    )?;

    let mut writer = super::uring::Writer::new_without_runtime(RuntimeFreeWriter::new(
        tmppath.path().into(),
        data.schema(),
        4,
        WriteOpts::default(),
    ));
    writer.write(data.clone(), 2).await?;
    writer.write(data.clone(), 1).await?;
    writer.write(data.clone(), 2).await?;
    let oom_data = writer.finish().await?;

    eprintln!("{:#?}", oom_data);

    let mut reader = super::uring::Reader::new(oom_data);
    let mut read_batches = vec![];
    for part in 0..4 {
        let mut stream = reader.launch(ReadOpts::default(), part);
        while let Some(batch) = stream.next().await {
            read_batches.push(batch?);
        }
    }

    compare_batches(read_batches.as_ref(), &vec![data; 3]);

    Ok(())
}

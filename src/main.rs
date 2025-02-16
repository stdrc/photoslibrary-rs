use anyhow::Context;
use futures::{pin_mut, StreamExt as _};
use photoslibrary::PhotosDb;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let library_path = std::env::args()
        .nth(1)
        .context("Usage: photoslibrary-cli <library-path>")?;
    let photosdb = PhotosDb::new(&library_path).await?;

    let photos = photosdb.visible_photos().await?;
    pin_mut!(photos);
    let mut p = vec![];
    while let Some(photo) = photos.next().await {
        let photo = photo?;
        p.push(photo);
        // println!(
        //     "{}: {}, {}, {}, {}",
        //     photo.pk,
        //     photo.extra().await.unwrap().original_filename,
        //     photo.created,
        //     photo.modified,
        //     photo.added
        // );
    }
    println!("count: {}", p.len());
    Ok(())
}

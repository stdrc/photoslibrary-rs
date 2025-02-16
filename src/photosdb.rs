use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use std::path::{Path, PathBuf};

pub struct PhotosDb {
    library_path: PathBuf,
    conn: libsql::Connection,
}

impl PhotosDb {
    pub async fn new<P: AsRef<Path>>(library_path: P) -> Result<Self> {
        let library_path = library_path.as_ref().to_path_buf();
        let db_path = library_path.join("database").join("Photos.sqlite");
        let db = libsql::Builder::new_local(db_path)
            .build()
            .await
            .context("failed to open database")?;
        let conn = db.connect().context("failed to connect to database")?;
        Ok(Self { library_path, conn })
    }

    pub async fn visible_photos(&self) -> Result<impl Stream<Item = Result<Photo>>> {
        let mut stmt = self.conn.prepare("
            SELECT
                ZASSET.Z_PK,
                ZASSET.ZUUID,
                ZADDITIONALASSETATTRIBUTES.ZORIGINALFILENAME,
                ZASSET.ZDATECREATED,
                ZASSET.ZMODIFICATIONDATE,
                ZASSET.ZDIRECTORY,
                ZASSET.ZFILENAME,
                ZASSET.ZKIND,
                ZASSET.ZUNIFORMTYPEIDENTIFIER,
                ZASSET.ZKINDSUBTYPE,
                ZASSET.ZHEIGHT,
                ZASSET.ZWIDTH
            FROM ZASSET JOIN ZADDITIONALASSETATTRIBUTES ON ZADDITIONALASSETATTRIBUTES.ZASSET = ZASSET.Z_PK
            WHERE ZASSET.ZHIDDEN = 0 AND ZASSET.ZVISIBILITYSTATE = 0
            ORDER BY ZASSET.Z_PK;
        "
        ).await.context("failed to prepare statement")?;
        let rows = stmt.query(()).await.context("failed to execute query")?;
        Ok(rows.into_stream().map(|res| {
            let row = res.context("Error reading photo")?;
            Ok(Photo {
                pk: row.get(0)?,
                uuid: row.get(1)?,
            })
        }))
    }
}

pub struct Photo {
    pub pk: i64,
    pub uuid: String,
}

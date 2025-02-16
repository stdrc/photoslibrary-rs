use anyhow::{Context, Result};
use chrono::DateTime;
use futures::{Stream, StreamExt};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::OnceCell;

const APPLE_EPOCH: i64 = 978307200;

pub struct PhotosDb {
    shared: Arc<SharedBlock>,
}

struct SharedBlock {
    _library_path: PathBuf,
    conn: libsql::Connection,
}

impl PhotosDb {
    pub async fn new<P: AsRef<Path>>(library_path: P) -> Result<Self> {
        let library_path = library_path.as_ref().to_path_buf();
        let db_path = library_path.join("database").join("Photos.sqlite");
        let db = libsql::Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;
        Ok(Self {
            shared: Arc::new(SharedBlock {
                _library_path: library_path,
                conn,
            }),
        })
    }

    pub async fn visible_photos(&self) -> Result<impl Stream<Item = Result<Photo>>> {
        let sql = r#"
            SELECT
                ZASSET.Z_PK,                   -- 0
                ZASSET.ZUUID,                  -- 1

                ZASSET.ZKIND,                  -- 2
                ZASSET.ZUNIFORMTYPEIDENTIFIER, -- 3
                ZASSET.ZKINDSUBTYPE,           -- 4

                ZASSET.ZDIRECTORY,             -- 5
                ZASSET.ZFILENAME,              -- 6

                ZASSET.ZDATECREATED,           -- 7
                ZASSET.ZMODIFICATIONDATE,      -- 8
                ZASSET.ZADDEDDATE,             -- 9

                ZASSET.ZHEIGHT,                -- 10
                ZASSET.ZWIDTH                  -- 11
            FROM ZASSET
            WHERE
                ZASSET.ZTRASHEDSTATE = 0 AND
                ZASSET.ZHIDDEN = 0 AND
                ZASSET.ZVISIBILITYSTATE = 0
            ORDER BY ZASSET.Z_PK
        "#;
        let mut stmt = self.shared.conn.prepare(sql).await?;
        let rows = stmt.query(()).await?;
        let shared_block = self.shared.clone();
        Ok(rows.into_stream().map(move |res| {
            let row = res?;

            let get_dt = |idx| {
                let s = row.get_str(idx)?;
                let apple_ts: f64 = s.parse().unwrap();
                let ts = apple_ts + APPLE_EPOCH as f64;
                let secs = ts.trunc() as i64;
                let nsecs = (ts.fract() * 1_000_000_000.0) as u32;
                Ok::<_, libsql::Error>(DateTime::from_timestamp(secs, nsecs))
            };

            let filename = row.get_str(6)?;
            Ok(Photo {
                shared: shared_block.clone(),
                pk: row.get(0)?,
                uuid: row.get(1)?,
                kind: PhotoKind::from_i32(row.get(2)?),
                uniform_type_identifier: row.get(3)?,
                kind_subtype: row.get(4)?,
                directory: row.get(5)?,
                filename: filename.to_owned(),
                created: get_dt(7)?
                    .with_context(|| format!("bad created datetime for file {}", filename))?,
                modified: get_dt(8)?
                    .with_context(|| format!("bad modified datetime for file {}", filename))?,
                added: get_dt(9)?
                    .with_context(|| format!("bad added datetime for file {}", filename))?,
                height: row.get(10)?,
                width: row.get(11)?,
                extra: OnceCell::new(),
            })
        }))
    }
}

pub struct Photo {
    shared: Arc<SharedBlock>,

    pub pk: i64,
    pub uuid: String,
    pub kind: PhotoKind,
    pub uniform_type_identifier: String,
    pub kind_subtype: i64,

    pub directory: String,
    pub filename: String,

    pub created: DateTime<chrono::Utc>,
    pub modified: DateTime<chrono::Utc>,
    pub added: DateTime<chrono::Utc>,

    pub height: i32,
    pub width: i32,

    extra: OnceCell<PhotoExtra>,
}

#[derive(Debug, Clone, Copy)]
pub enum PhotoKind {
    Photo,
    Video,
    Unknown(i32),
}

impl PhotoKind {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Photo,
            1 => Self::Video,
            _ => Self::Unknown(value),
        }
    }
}

pub struct PhotoExtra {
    pub original_filename: String,
}

impl Photo {
    pub async fn extra(&self) -> Result<&PhotoExtra> {
        Ok(self
            .extra
            .get_or_try_init(|| async {
                let sql = r#"
                    SELECT
                        ZADDITIONALASSETATTRIBUTES.ZORIGINALFILENAME
                    FROM ZADDITIONALASSETATTRIBUTES
                    WHERE
                        ZADDITIONALASSETATTRIBUTES.ZASSET = ?
                "#;
                let mut stmt = self.shared.conn.prepare(sql).await?;
                let row = stmt.query_row(&[self.pk]).await?;
                Ok::<_, libsql::Error>(PhotoExtra {
                    original_filename: row.get(0)?,
                })
            })
            .await?)
    }
}

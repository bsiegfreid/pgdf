use chrono::NaiveDate;
use polars::prelude::*;
use tokio_postgres::{types::Type, Row};
use std::error::Error as StdError;

pub async fn postgres_to_polars(rows: &[Row]) -> Result<DataFrame, Box<dyn StdError>> {
    if rows.is_empty() {
        return Err(Box::new(PolarsError::NoData("No data in rows".into())));
    }

    let mut df = DataFrame::default(); // Start with an empty DataFrame
    let column_count = rows[0].len();

    for col_index in 0..column_count {
        let field_name: &str = rows[0].columns()[col_index].name();

        let column_type = rows[0].columns()[col_index].type_();

        match *column_type {

            Type::VARCHAR | Type::TEXT => {
                let vals: Vec<Option<String>> = rows.iter()
                    .map(|row| row.try_get::<_, &str>(col_index).ok().map(|v| v.to_string()))
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }

            Type::INT4 => {
                let vals: Vec<Option<i32>> = rows.iter()
                    .map(|row| row.try_get(col_index).ok())
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }

            Type::FLOAT8 => {
                let vals: Vec<Option<f64>> = rows.iter()
                    .map(|row| row.try_get(col_index).ok())
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }

            Type::BOOL => {
                let vals: Vec<Option<bool>> = rows.iter()
                    .map(|row| row.try_get(col_index).ok())
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }

            Type::DATE => {
                let vals: Vec<Option<NaiveDate>> = rows.iter()
                    .map(|row| row.try_get::<_, NaiveDate>(col_index).ok())
                    .collect();
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let vals: Vec<Option<i32>> = vals.iter()
                    .map(|val| val.map(|d| (d.signed_duration_since(epoch)).num_days() as i32))
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }

            Type::VARCHAR_ARRAY | Type::TEXT_ARRAY => {
                let vals: Vec<Option<String>> = rows.iter()
                    .map(|row| {
                        row.try_get::<_, Vec<&str>>(col_index)
                            .ok()
                            .map(|v| serde_json::to_string(&v).unwrap())
                    })
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }

            Type::INT4_ARRAY => {
                let vals: Vec<Option<Vec<i32>>> = rows.iter()
                    .map(|row| row.try_get(col_index).ok())
                    .collect();
                // Convert Vec<Option<Vec<i32>>> to ListChunked
                let s = vals
                    .into_iter()
                    .map(|opt_vec| opt_vec.map(|v| Series::new("".into(), v)))
                    .collect::<ListChunked>();
                let mut s = s.into_series();
                s.rename(field_name.into());
                df.with_column(s)?;
            }

            Type::FLOAT8_ARRAY => {
                let vals: Vec<Option<Vec<f64>>> = rows.iter()
                    .map(|row| row.try_get(col_index).ok())
                    .collect();
                // Convert Vec<Option<Vec<f64>>> to ListChunked
                let s = vals
                    .into_iter()
                    .map(|opt_vec| opt_vec.map(|v| Series::new("".into(), v)))
                    .collect::<ListChunked>();
                let mut s = s.into_series();
                s.rename(field_name.into());
                df.with_column(s)?;
            }

            _ => {
                // Fallback: try to get as string, or None if not possible
                let vals: Vec<Option<String>> = rows.iter()
                    .map(|row| row.try_get::<_, &str>(col_index).ok().map(|v| v.to_string()))
                    .collect();
                df.with_column(Series::new(field_name.into(), vals))?;
            }
        }
    }

    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::{NoTls, Row};


    // Helper to create a fake Row with given values and types
    fn make_row<'a>(columns: Vec<(&'a str, Type)>, values: Vec<Option<&'a (dyn tokio_postgres::types::ToSql + Sync)>>) -> Row {
        // This is a stub. In real tests, you would use a test Postgres instance or a mock.
        unimplemented!("Row mocking is non-trivial; use a test database for integration tests.")
    }

    #[tokio::test]
    async fn test_empty_rows_returns_error() {
        let rows: Vec<Row> = vec![];
        let result = postgres_to_polars(&rows).await;
        assert!(result.is_err());
    }

    // The following tests are integration tests and require a running Postgres instance.
    // They are skipped by default unless you set up a test database.

    #[tokio::test]
    async fn test_single_int_column() {
        let (client, connection) = tokio_postgres::connect("host=localhost user=postgres dbname=postgres", NoTls).await.unwrap();
        tokio::spawn(connection);
        client.batch_execute("CREATE TEMP TABLE test (id INT)").await.unwrap();
        client.execute("INSERT INTO test (id) VALUES ($1)", &[&42i32]).await.unwrap();
        let rows = client.query("SELECT id FROM test", &[]).await.unwrap();
        let df = postgres_to_polars(&rows).await.unwrap();
        assert_eq!(df.shape(), (1, 1));
        assert_eq!(df.column("id").unwrap().i32().unwrap().get(0), Some(42));
    }

    // #[tokio::test]
    // async fn test_multiple_types() {
    //     let (client, connection) = tokio_postgres::connect("host=localhost user=postgres dbname=postgres", NoTls).await.unwrap();
    //     tokio::spawn(connection);
    //     client.batch_execute("CREATE TEMP TABLE test2 (id INT, name TEXT, active BOOL, score FLOAT8, created DATE)").await.unwrap();
    //     client.execute(
    //         "INSERT INTO test2 (id, name, active, score, created) VALUES ($1, $2, $3, $4, $5)",
    //         &[&1i32, &"Alice", &true, &99.5f64, &NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()]
    //     ).await.unwrap();
    //     let rows = client.query("SELECT id, name, active, score, created FROM test2", &[]).await.unwrap();
    //     let df = postgres_to_polars(&rows).await.unwrap();
    //     assert_eq!(df.shape(), (1, 5));
    //     assert_eq!(df.column("name").unwrap().utf8().unwrap().get(0), Some("Alice"));
    //     assert_eq!(df.column("active").unwrap().bool().unwrap().get(0), Some(true));
    //     assert_eq!(df.column("score").unwrap().f64().unwrap().get(0), Some(99.5));
    //     assert_eq!(df.column("created").unwrap().i32().unwrap().get(0), Some(19358)); // days since 1970-01-01
    // }

    // #[tokio::test]
    // async fn test_array_columns() {
    //     let (client, connection) = tokio_postgres::connect("host=localhost user=postgres dbname=postgres", NoTls).await.unwrap();
    //     tokio::spawn(connection);
    //     client.batch_execute("CREATE TEMP TABLE test3 (tags TEXT[], nums INT[], floats FLOAT8[])").await.unwrap();
    //     client.execute(
    //         "INSERT INTO test3 (tags, nums, floats) VALUES ($1, $2, $3)",
    //         &[&vec!["a", "b"], &vec![1i32, 2], &vec![1.1f64, 2.2]]
    //     ).await.unwrap();
    //     let rows = client.query("SELECT tags, nums, floats FROM test3", &[]).await.unwrap();
    //     let df = postgres_to_polars(&rows).await.unwrap();
    //     assert_eq!(df.shape(), (1, 3));
    //     let tags_series = df.column("tags").unwrap().list().unwrap().get(0).unwrap();
    //     let tags_utf8 = tags_series.utf8().unwrap();
    //     let tags_vec: Vec<&str> = tags_utf8.into_no_null_iter().collect();
    //     assert!(tags_vec.contains(&"a") && tags_vec.contains(&"b"));
    //     let nums_series = df.column("nums").unwrap().list().unwrap().get(0).unwrap();
    //     assert_eq!(nums_series.i32().unwrap().into_no_null_iter().collect::<Vec<_>>(), vec![1, 2]);
    //     let floats = df.column("floats").unwrap().list().unwrap().get(0).unwrap();
    //     let float_series = floats.f64().unwrap();
    //     assert_eq!(float_series.into_no_null_iter().collect::<Vec<_>>(), vec![1.1, 2.2]);
    // }

    #[tokio::test]
    async fn test_null_values() {
        let (client, connection) = tokio_postgres::connect("host=localhost user=postgres dbname=postgres", NoTls).await.unwrap();
        tokio::spawn(connection);
        client.batch_execute("CREATE TEMP TABLE test4 (id INT, name TEXT)").await.unwrap();
        client.execute("INSERT INTO test4 (id, name) VALUES ($1, $2)", &[&Option::<i32>::None, &Option::<&str>::None]).await.unwrap();
        let rows = client.query("SELECT id, name FROM test4", &[]).await.unwrap();
        let df = postgres_to_polars(&rows).await.unwrap();
        assert_eq!(df.shape(), (1, 2));
        assert!(df.column("id").unwrap().i32().unwrap().get(0).is_none());
        assert!(df.column("name").unwrap().str().unwrap().get(0).is_none());
    }
}
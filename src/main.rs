use csv::Reader;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use tokio::fs;

#[derive(Debug, Deserialize)]
struct CsvRecord {
    #[serde(rename = "SERIAL NUMBER")]
    serial_number: String,
    #[serde(rename = "GRADER")]
    grader: String,
    #[serde(rename = "TITLE")]
    title: String,
    #[serde(rename = "YEAR")]
    year: String,
    #[serde(rename = "LANGUAGE")]
    language: String,
    #[serde(rename = "SET")]
    set: String,
    #[serde(rename = "GRADE")]
    grade: String,
    #[serde(rename = "video")]
    video: String,
    #[serde(rename = "image")]
    image: String,
    #[serde(rename = "external_url")]
    external_url: String,
}

#[derive(Debug, Serialize)]
struct Attribute {
    trait_type: String,
    value: Value,
    display_type: String,
}

#[derive(Debug, Serialize)]
struct Metadata {
    name: String,
    description: String,
    image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    video: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_url: Option<String>,
    attributes: Vec<Attribute>,
}

impl From<CsvRecord> for Metadata {
    fn from(record: CsvRecord) -> Self {
        let mut attributes = Vec::new();

        // Add Grader attribute if not empty
        if !record.grader.trim().is_empty() {
            attributes.push(Attribute {
                trait_type: "Grader".to_string(),
                value: Value::String(record.grader.trim().to_string()),
                display_type: "string".to_string(),
            });
        }

        // Add Serial Number attribute if not empty
        if !record.serial_number.trim().is_empty() {
            attributes.push(Attribute {
                trait_type: "Serial Number".to_string(),
                value: Value::String(record.serial_number.trim().to_string()),
                display_type: "string".to_string(),
            });
        }

        // Add Grade attribute if not empty
        if !record.grade.trim().is_empty() {
            attributes.push(Attribute {
                trait_type: "Grade".to_string(),
                value: Value::String(record.grade.trim().to_string()),
                display_type: "string".to_string(),
            });
        }

        // Add Year attribute if not empty
        if !record.year.trim().is_empty() {
            attributes.push(Attribute {
                trait_type: "Year".to_string(),
                value: Value::String(record.year.trim().to_string()),
                display_type: "string".to_string(),
            });
        }

        // Add Language attribute if not empty
        if !record.language.trim().is_empty() {
            attributes.push(Attribute {
                trait_type: "Language".to_string(),
                value: Value::String(record.language.trim().to_string()),
                display_type: "string".to_string(),
            });
        }

        // Add Set attribute if not empty
        if !record.set.trim().is_empty() {
            attributes.push(Attribute {
                trait_type: "Set".to_string(),
                value: Value::String(record.set.trim().to_string()),
                display_type: "string".to_string(),
            });
        }

        let video = if record.video.trim().is_empty() {
            None
        } else {
            Some(record.video.trim().to_string())
        };

        let external_url = if record.external_url.trim().is_empty() {
            None
        } else {
            Some(record.external_url.trim().to_string())
        };

        Metadata {
            name: record.title.clone(),
            description: record.title,
            image: record.image.trim().to_string(),
            video,
            external_url,
            attributes,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting CSV parsing and metadata generation...");

    // First, count the total number of records in the CSV file
    let file_for_counting = File::open("final.csv")?;
    let mut rdr_for_counting = Reader::from_reader(file_for_counting);
    let total_records = rdr_for_counting.deserialize::<CsvRecord>().count();
    println!("Found {} records in CSV file", total_records);

    // Open and read the CSV file for processing
    let file = File::open("final.csv")?;
    let mut rdr = Reader::from_reader(file);

    let mut count = 1;

    // Process each record
    for result in rdr.deserialize() {
        let record: CsvRecord = result?;
        let metadata: Metadata = record.into();

        // Serialize to JSON with pretty formatting
        let json_output = serde_json::to_string_pretty(&metadata)?;

        // Write to file named as count (1, 2, 3, etc.)
        let filename = format!("{}", count);
        fs::write(&filename, json_output).await?;

        println!("Generated metadata file: {}", filename);
        count += 1;
    }

    println!("Successfully generated {} metadata files!", count - 1);
    Ok(())
}

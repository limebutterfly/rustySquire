use std::env::args;
use std::process::exit;
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::mem::take;
use std::thread;
#[derive(Clone)]
struct FileDefinition {
    postfix: String,
    skip: usize,
    rowid_col: usize,
    metadata_cols: Vec<usize>,
    value_col: usize,
    ncols: usize
}

struct LongData {
    rowids: Vec<String>,
    colid: String,
    metadata: Vec<Vec<String>>,
    values: Vec<f32>
}

impl LongData {

    fn read_file_inner(file_path: PathBuf, filename: String, filetype: FileDefinition) -> Self {
        println!("processing file {}",filename);
        let mut file_data = LongData::new(filename);
        for line in BufReader::new(File::open(file_path).expect("could not open file.")).lines().skip(filetype.skip) {
            let line_a: Vec<String> = line.unwrap().split("\t").map(|s| s.to_string()).collect();
            if line_a.len() < filetype.ncols {continue;}
            let rowid = line_a[filetype.rowid_col].parse::<String>().unwrap();
            let value = line_a[filetype.value_col].parse::<f32>().unwrap_or_else(|_| 0.0);
            let mut metadata : Vec<String> = vec!["".parse().unwrap(); filetype.metadata_cols.len()];
            for i in 0..filetype.metadata_cols.len() {
                metadata[i] = line_a[filetype.metadata_cols[i]].parse::<String>().unwrap();
            }
            file_data.add_row(rowid, metadata, value);
        }
        file_data.sort();
        file_data
    }
    fn read_file(file_path: &PathBuf, filename: String, filetype: FileDefinition) -> thread::JoinHandle<Self> {
        let fp = file_path.clone();
        thread::spawn(move || {
            Self::read_file_inner(fp, filename, filetype)
        })
    }

    pub fn new(colid: String) -> Self {
        Self { rowids: Vec::new(), colid: colid, metadata: Vec::new(), values: Vec::new() }
    }

    pub fn add_row(&mut self, rowid: String, metadata: Vec<String>, value: f32) {
        self.rowids.push(rowid);
        self.metadata.push(metadata);
        self.values.push(value);
    }

    pub fn sort(&mut self) {
        assert_eq!(self.len(), self.metadata.len());
        assert_eq!(self.len(), self.values.len());

        let mut indices: Vec<usize> = (0..self.len()).collect();
        indices.sort_by_key(|&i| &self.rowids[i]);
        let new_row_ids = indices
            .iter()
            .map(|&i| take(&mut self.rowids[i]))
            .collect();
        self.rowids = new_row_ids;
        let new_metadata = indices
            .iter()
            .map(|&i| take(&mut self.metadata[i]))
            .collect();
        self.metadata = new_metadata;
        let new_values = indices
            .iter()
            .map(|&i| take(&mut self.values[i]))
            .collect();
        self.values = new_values;
    }

    pub fn len(&self) -> usize {
        self.rowids.len()
    }
}

struct WideData {
    rowids: Vec<String>,
    colids: Vec<String>,
    metadata: Vec<Vec<String>>,
    values: Vec<Vec<f32>>,
}

impl WideData {
    pub fn new() -> Self {
        Self { rowids: Vec::new(), colids: Vec::new(), metadata: Vec::new(), values: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.rowids.len()
    }

    pub fn add_row_data(&mut self, row_data: &mut LongData) {
            if self.len() == 0 {
            self.values.push(take(&mut row_data.values));
            self.colids.push(take(&mut row_data.colid));
            self.rowids = take(&mut row_data.rowids);
            self.metadata = take(&mut row_data.metadata);
            return;
        }
        let mut existing_values: Vec<f32> = vec![0.0; self.len()];
        let mut new_values: Vec<f32> = Vec::new();
        let mut new_rownames: Vec<String> = Vec::new();
        let mut new_metadata: Vec<Vec<String>> = Vec::new();
        let mut last_position: usize = 0;

        for i in 0..row_data.len() {
            if row_data.values[i] == 0.0 {
                continue; //dont add zero values
            }
            let mut row_processed = false;
            for j in last_position..self.len() {
                if self.rowids[j] == row_data.rowids[i] {
                    existing_values[j] = row_data.values[i];
                    last_position = j;
                    row_processed = true;
                    break;
                }
                if self.rowids[j] > row_data.rowids[i] {
                    last_position = j;
                    break;
                }
            }
            if ! row_processed {
                new_values.push(row_data.values[i]);
                new_rownames.push(row_data.rowids[i].clone());
                new_metadata.push(row_data.metadata[i].clone());
            }

        }
        let existing_values_length = existing_values.len();
        self.values.push([existing_values, new_values].concat());
        self.rowids.append(&mut new_rownames);
        self.metadata.append(&mut new_metadata);
        self.colids.push(row_data.colid.clone());
        self.sort(existing_values_length);
    }


    pub fn sort(&mut self, existing_values_length: usize) {
        // sort this, but assume everything is already sorted until new_values_length
        if existing_values_length == 0 {return}
        if existing_values_length == self.len() {return}
        let mut indices : Vec<usize> = (0..existing_values_length).collect();
        let mut old_value_start = 0;
        let mut n_inserted = 0;
        for new_value in existing_values_length..self.len() {
            let mut new_value_has_been_inserted = false;
            for old_value in old_value_start..existing_values_length {
                if self.rowids[new_value] < self.rowids[old_value+n_inserted] {
                    indices.insert(old_value+n_inserted, new_value);
                    old_value_start = old_value+1;
                    new_value_has_been_inserted = true;
                    n_inserted += 1;
                    break;
                }
            }
            if ! new_value_has_been_inserted {
                indices.push(new_value);
            }
        }
        let new_row_ids = indices
            .iter()
            .map(|&i| take(&mut self.rowids[i]))
            .collect();
        self.rowids = new_row_ids;
        let new_metadata = indices
            .iter()
            .map(|&i| take(&mut self.metadata[i]))
            .collect();
        self.metadata = new_metadata;
        for i in 0..self.values.len() {
            if self.values[i].len() < self.rowids.len() {
                let len = self.values[i].len();
                self.values[i].extend((0..self.rowids.len()-len).map(|_| 0.0));
            }
            let new_values= indices
                .iter()
                .map(|&j| take(&mut self.values[i][j]))
                .collect();
            self.values[i] = new_values;
        }
    }

    pub fn print_wide(& self, mut output: BufWriter<File>)  {
        output.write(format!("row_id\t").as_bytes()).unwrap();
        if self.metadata.len()>0 {
            for i in 0..self.metadata[0].len() {
                output.write(format!("metadata_{}\t", i).as_bytes()).unwrap();
            }
        }
        output.write(self.colids.join("\t").as_bytes()).unwrap();
        output.write("\n".as_bytes()).unwrap();
        for i in 0..self.rowids.len() {
            output.write(self.rowids[i].as_bytes()).unwrap();
            output.write("\t".as_bytes()).unwrap();
            output.write(self.metadata[i].join("\t").as_bytes()).unwrap();
            if self.metadata[i].len() == 1 {
                output.write("\t".as_bytes()).unwrap();
            }
            for j in 0..self.colids.len() {
                if self.values[j].len() <= i {
                    output.write("0".as_bytes()).unwrap();
                } else {
                    output.write(self.values[j][i].to_string().as_bytes()).unwrap();
                }
                output.write("\t".as_bytes()).unwrap();
            }
            output.write("\n".as_bytes()).unwrap();
        }
    }
}

fn aggregate(dir_path: &Path, filetype: FileDefinition) -> WideData {
    let mut output_data = WideData::new();
    let mut lastfile: Option<thread::JoinHandle<LongData>> = None;
    for path in fs::read_dir(&dir_path).unwrap() {
        let upath = path.unwrap();
        let filename = upath.file_name().into_string().unwrap();
        if filename.len() > filetype.postfix.len() && filename[filename.len()-filetype.postfix.len()..filename.len()].eq(&filetype.postfix) {

            let filename = filename[0..filename.len()-filetype.postfix.len()].parse().unwrap();
            // first fire off the new file to be read in in a differen thread
            let newfile = LongData::read_file(&upath.path(), filename, filetype.clone());
            // then actually merge the last file.
            match lastfile {
                Some(_) => {
                    let mut long_data = lastfile.unwrap().join().unwrap();
                    output_data.add_row_data(&mut long_data);
                }
                None => {}
            }
            lastfile = Some(newfile);
        }
    }
    match lastfile {
        Some(_) => {
            let mut long_data = lastfile.unwrap().join().unwrap();
            output_data.add_row_data(&mut long_data);
        }
        None => {}
    }
    return output_data;
}

fn main() {
    let args: Vec<String> = args().collect();
    let dir_path;
    if args.len() < 2 {
        println!("Usage: {} <directory_path>", args[0]);
        //        exit(1);
        dir_path = Path::new("../output/GSE87631");
        /*dir_path = Path::new("test_directory");
        let test_counts = FileDefinition {
            postfix: "_A.txt".parse().unwrap(),
            rowid_col: 0,
            metadata_cols: vec!(),
            value_col: 1,
            skip: 1,
            ncols: 2
        };
        aggregate(&dir_path, &test_counts).print_wide(BufWriter::new(File::create(format!("{}.test.tsv", dir_path.display())).unwrap()));
        exit(0); */
    } else {
        dir_path = Path::new(&args[1]);
    }
    if ! dir_path.is_dir() {
        println!("{} is not a directory", dir_path.display());
        exit(1);
    }
    let sub_f_counts = FileDefinition {
        postfix: "_subFcounts.txt".parse().unwrap(),
        rowid_col: 2,
        metadata_cols: vec!(3),
        value_col: 6,
        skip: 1,
        ncols: 8
    };
    let ref_gene_counts = FileDefinition {
        postfix: "_refGenecounts.txt".parse().unwrap(),
        rowid_col: 3,
        metadata_cols: vec!(0,1,2),
        value_col: 6,
        skip: 0,
        ncols: 7
    };
    let te_counts = FileDefinition {
        postfix: "_TEcounts.txt".parse().unwrap(),
        rowid_col: 3,
        metadata_cols: vec!(8,9,10,11,12),
        value_col: 15,
        skip: 1,
        ncols: 17
    };
    println!("processing subFcounts");
    aggregate(&dir_path, sub_f_counts).print_wide(BufWriter::new(File::create(format!("{}.subFcounts.tsv", dir_path.display())).unwrap()));
    println!("processing refGenecounts");
    aggregate(&dir_path, ref_gene_counts).print_wide(BufWriter::new(File::create(format!("{}.refGenecounts.tsv", dir_path.display())).unwrap()));
    println!("processing TEcounts");
    aggregate(&dir_path, te_counts).print_wide(BufWriter::new(File::create(format!("{}.TEcounts.tsv", dir_path.display())).unwrap()));
    println!("done.")

}

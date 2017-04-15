

use std::error::Error;
use byteorder::{ByteOrder, LittleEndian};
use std::ops::Index;
use std::rc::Rc;


pub struct DBF {
    last_modified: Date,
    fields: Rc<Vec<FieldDescriptor>>,
    records: Vec<Record>,
}

#[derive(Debug)]
pub struct FieldDescriptor {
    pub name: String,
    pub field_type: u8, //ascii char
    pub field_length: u8,
    pub field_start: u16,
}

#[derive(Debug, PartialEq)]
pub struct Date {
    pub year: u32,
    pub month: u8,
    pub day: u8,
}

pub struct Record {
    data: Vec<u8>,
    fields: Rc<Vec<FieldDescriptor>>,
}


#[derive(Debug, PartialEq)]
pub enum RecordField {
    Text(String),
    Number(f64),
    Date(Date),
    Bool(bool),
}


impl DBF {
    pub fn from_file(filename: &str) -> Result<Self, Box<Error>> {
        use std::fs::File;
        use std::io::prelude::*;
        use std::io::SeekFrom;
        use std::mem;

        let mut f = File::open(filename)?;
        //we're just reading into it, so leave it uninitialized
        let mut header_start: [u8; 32] = unsafe { mem::uninitialized() };
        f.read_exact(&mut header_start)?;

        let date = parse_date_binary(&header_start[1..4]);


        let num_records = LittleEndian::read_u32(&header_start[4..8]);
        let num_header_bytes = LittleEndian::read_u16(&header_start[8..10]);
        let bytes_per_record = LittleEndian::read_u16(&header_start[10..12]);

        let num_fields = (num_header_bytes - 33) / 32;

        let mut fields = Vec::with_capacity(num_fields as usize);
        let mut field_byte_offset: u16 = 0;
        for _ in 0..num_fields {
            //don't initialize if we're just going to read into it
            let mut fd_buffer: [u8; 32] = unsafe { mem::uninitialized() };
            f.read_exact(&mut fd_buffer)?;

            let field_name = unsafe { str_from_u8_nul_utf8(&fd_buffer[..11]) };
            let field_length = fd_buffer[16];
            let field_type = fd_buffer[11];
            match field_type {
                b'C' | b'D' | b'F' | b'L' | b'M' | b'N' => (),
                _ => return Err(From::from("invalid field type")),
            }

            fields.push(FieldDescriptor {
                            name: String::from(field_name),
                            field_type: field_type,
                            field_length: field_length,
                            field_start: field_byte_offset,
                        });
            field_byte_offset += field_length as u16;
        }
        let records = Vec::with_capacity(num_records as usize);


        let mut dbf = DBF {
            last_modified: date,
            fields: Rc::new(fields),
            records: records,
        };
        //seek to the start of the records
        f.seek(SeekFrom::Start(num_header_bytes as u64 + 1))?;
        for _ in 0..num_records {
            //create uninitialized buffer
            let mut record_buf = Vec::with_capacity(bytes_per_record as usize);
            unsafe { record_buf.set_len(bytes_per_record as usize) };
            f.read_exact(&mut record_buf)?;
            dbf.records
                .push(Record {
                          data: record_buf,
                          fields: dbf.fields.clone(),
                      });
        }


        Ok(dbf)
    }

    pub fn last_modified(&self) -> &Date {
        &self.last_modified
    }

    pub fn fields(&self) -> &[FieldDescriptor] {
        self.fields.as_slice()
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    pub fn iter_records(&self) -> RecordsIterator {
        RecordsIterator {
            parent: self,
            index: 0,
        }
    }
}

impl Index<usize> for DBF {
    type Output = Record;
    fn index(&self, index: usize) -> &Self::Output {
        &self.records[index]
    }
}

pub struct RecordsIterator<'a> {
    parent: &'a DBF,
    index: usize,
}

impl<'a> Iterator for RecordsIterator<'a> {
    type Item = &'a Record;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.parent.records.len() {
            self.index += 1;
            Some(&self.parent[self.index - 1])
        } else {
            None
        }
    }
}


//via https://stackoverflow.com/questions/42066381/
//how-to-get-a-str-from-a-nul-terminated-byte-slice-if-the-nul-terminator-isnt
unsafe fn str_from_u8_nul_utf8(utf8_src: &[u8]) -> &str {
    let nul_range_end = utf8_src
        .iter()
        .position(|&c| c == b'\0')
        .unwrap_or(utf8_src.len()); // default to length if no `\0` present
    ::std::str::from_utf8_unchecked(&utf8_src[0..nul_range_end])
}

unsafe fn str_from_u8_ws_padded(utf8_src: &[u8]) -> &str {
    let first_non_space = utf8_src.iter().position(|&c| c != b' ').unwrap_or(0);
    ::std::str::from_utf8_unchecked(&utf8_src[first_non_space..])
}

fn parse_date_binary(buffer: &[u8]) -> Date {
    Date {
        year: 1900 + buffer[0] as u32,
        month: buffer[1],
        day: buffer[2],
    }
}

fn parse_date_text(buffer: &[u8]) -> Date {
    use std::str;
    Date {
        year: str::from_utf8(&buffer[..4]).unwrap().parse().unwrap(),
        month: str::from_utf8(&buffer[4..6]).unwrap().parse().unwrap(),
        day: str::from_utf8(&buffer[6..8]).unwrap().parse().unwrap(),
    }
}

impl Record {
    pub fn field_by_index(&self, index: usize) -> RecordField {
        use std::str;
        let ref fields = self.fields;
        let start = fields[index].field_start as usize;
        let end = start + fields[index].field_length as usize;

        let field_slice = &self.data[start..end];

        match fields[index].field_type {
            b'C' | b'M' => {
                unsafe {
                    RecordField::Text(String::from(str_from_u8_ws_padded(field_slice).trim()))
                }
            }

            b'D' => RecordField::Date(parse_date_text(field_slice)),
            b'F' | b'N' => unsafe {
                RecordField::Number(str_from_u8_ws_padded(field_slice)
                                        .trim()
                                        .parse()
                                        .unwrap())

            },
            b'L' => {
                RecordField::Bool(field_slice[0] == b'Y' || field_slice[0] == b'y' ||
                                  field_slice[0] == b'T' ||
                                  field_slice[0] == b't')
            }
            _ => panic!(),

        }
    }

    pub fn field_by_name(&self, field_name: &str) -> Option<RecordField> {
        let field_index = self.fields
            .iter()
            .position(|ref s| s.name == field_name);
        field_index.map(|i| self.field_by_index(i))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dbf_test() {
        let dbf = DBF::from_file("test_inputs/test_dbf.dbf").unwrap();
        assert_eq!(dbf.last_modified.year, 2016);
        assert_eq!(dbf.last_modified.month, 2);
        assert_eq!(dbf.last_modified.day, 17);

        for f in dbf.fields.iter() {
            println!("field: {:?}", f);
        }

        assert_eq!(dbf.fields.len(), 9);
        assert_eq!(dbf.fields[0].name, "STATEFP");
        assert_eq!(dbf.fields[0].field_type, b'C');

        assert_eq!(dbf[25].field_by_name("NAME").unwrap(),
                   RecordField::Text(String::from("Colorado")));

        for rec in dbf.iter_records().take(5) {
            for i in 0..dbf.num_fields() {
                println!("field number {} : {:?}", i, rec.field_by_index(i));
            }
        }


    }

}


extern crate byteorder;
use self::byteorder::{ByteOrder, LittleEndian, BigEndian};
use std::error::Error;
use enum_primitive::FromPrimitive;
use std::f64;



pub struct ShapeFile {
    pub bounding_box: BoundingBox,
    shape_type: ShapeType,
    shapes: Vec<Shape>
}

#[derive(Debug, Copy, Clone)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

pub struct BoundingBox {
    pub min: Point,
    pub max: Point,
}



enum_from_primitive! {
    #[derive(Debug, PartialEq)]
    pub enum ShapeType{
    Null = 0,
    Point = 1,
    Polyline = 3,
    Polygon = 5,
    MultiPoint = 6,
    PointZ = 11,
    PolylineZ = 13,
    PolygonZ = 15,
    MultiPointZ = 18,
    PointM = 21,
    PolylineM = 23,
    PolygonM = 25,
    MultiPointM = 28,
    MultiPatch = 31
}
}

//Works for all the 2D shape types
pub struct Shape {
    shape_type: ShapeType,
    bounding_box: BoundingBox,
    points: Vec<Point>,
    parts: Vec<(usize, usize)>,
    //todo Z and M stuff
}

impl ShapeFile {
    pub fn from_file(filename: &str) -> Result<Self, Box<Error>> {
        use std::fs;
        use std::fs::File;
        use std::io::prelude::*;
        use std::io::SeekFrom;
        use std::mem;

        let mut f = File::open(filename)?;

        let mut header: [u8; 100] = unsafe { mem::uninitialized() };
        f.read_exact(&mut header)?;

        if BigEndian::read_u32(&header[..4]) != 0x270a {
            return Err(From::from("invalid .shp file, magic number is wrong"));
        }

        let file_length = BigEndian::read_u32(&header[24..28]) * 2;
        let metadata = fs::metadata(filename).unwrap();
        if metadata.len() != file_length as u64 {
            return Err(From::from("file length field doesn't match header"));
        }

        let shape_type = ShapeType::from_i32(LittleEndian::read_i32(&header[32..36]));
        if let None = shape_type {
            return Err(From::from("invalid shape type"));
        }
        let shape_type = shape_type.unwrap();

        let bounding_box = BoundingBox::from_bytes(&header[36..68]);

        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;
        let mut shapes  = Vec::new();
        let mut buffer_slice = buffer.as_slice();
        while !buffer_slice.is_empty() {
            let shape = Shape::from_bytes(&mut buffer_slice)?;
            shapes.push(shape);
        }
        Ok(ShapeFile{
            bounding_box: bounding_box,
            shape_type: shape_type,
            shapes: shapes,
            
        })
    }
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x: x, y: y }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            x: LittleEndian::read_f64(&bytes[0..8]),
            y: LittleEndian::read_f64(&bytes[8..16]),
        }
    }
}

impl BoundingBox {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            min: Point::new(LittleEndian::read_f64(&bytes[0..8]),
                            LittleEndian::read_f64(&bytes[8..16])),
            max: Point::new(LittleEndian::read_f64(&bytes[16..24]),
                            LittleEndian::read_f64(&bytes[24..32])),
        }
    }

    pub fn from_point(p: Point) -> Self {
        Self { min: p, max: p }
    }

    pub fn nans() -> Self {
        Self {
            min: Point::new(f64::NAN, f64::NAN),
            max: Point::new(f64::NAN, f64::NAN),
        }
    }
}

fn read_points(bytes: &[u8], num_points: usize) -> Vec<Point> {

    let mut points = Vec::with_capacity(num_points as usize);
    for i in 0..num_points {
        let start = (16 * i) as usize;
        let stop = start + 16;
        points.push(Point::from_bytes(&bytes[start..stop]));
    }
    points
}

fn read_parts(bytes: &[u8], num_parts: usize, num_points: usize) -> Vec<(usize, usize)> {
    let mut parts: Vec<(usize, usize)> = Vec::with_capacity(num_parts);
    for i in 0..(num_parts - 1) {

        parts.push((LittleEndian::read_i32(&bytes[(4 * i)..(4 * (i + 1))]) as usize,
                    LittleEndian::read_i32(&bytes[(4 * (i + 1))..(4 * (i + 2))]) as usize));
    }
    if parts.is_empty() {
        parts.push((0, num_points));
    } else {
    let last_int = parts.last().unwrap().1;
        parts.push((last_int, num_points));
    }
    parts
}

impl Shape {
    //mutable because we'll cut chop off this shape's bytes before returning
    pub fn from_bytes(bytes:  &mut &[u8]) -> Result<Self, Box<Error>> {

        let record_length = 2 * BigEndian::read_i32(&bytes[4..8]);
        let shape_type = ShapeType::from_i32(LittleEndian::read_i32(&bytes[8..12]));
        if shape_type.is_none() {
            return Err(From::from("invalid shape type"));
        }
        let shape_type: ShapeType = shape_type.unwrap();

        let bb: BoundingBox;
        let points: Vec<Point>;
        let parts: Vec<(usize, usize)>;

        match shape_type {
            ShapeType::Null => {
                bb = BoundingBox::nans();
                points = vec![];
                parts = vec![];
            }
            ShapeType::Point => {
                let p = Point::from_bytes(&bytes[12..28]);
                bb = BoundingBox::from_point(p);
                points = vec![p];
                parts = vec![(0, 1)];
            }
            ShapeType::MultiPoint => {
                bb = BoundingBox::from_bytes(&bytes[12..44]);
                let num_points = LittleEndian::read_i32(&bytes[44..48]) as usize;
                points = read_points(&bytes[48..], num_points);
                parts = vec![(0, points.len())];

            }
            ShapeType::Polyline | ShapeType::Polygon => {
                bb = BoundingBox::from_bytes(&bytes[12..44]);

                let num_parts = LittleEndian::read_i32(&bytes[44..48]) as usize;
                let num_points = LittleEndian::read_i32(&bytes[48..52]) as usize;
                parts = read_parts(&bytes[52..], num_parts, num_points);
                let points_start = (52 + 4 * num_parts) as usize;
                points = read_points(&bytes[points_start..], num_points);
                
            }
            _ => return Err(From::from("shape type not implemented yet"))
        }
        *bytes = bytes.split_at((record_length + 8) as usize).1;
        Ok(Self {
               shape_type: shape_type,
               bounding_box: bb,
               points: points,
               parts: parts,
           })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shapefile_test() {
        let shapefile = ShapeFile::from_file("test_inputs/states.shp").unwrap();

        println!("num shapes: {}", shapefile.shapes.len());
        println!("colorado, points: ");
        println!("{:?}", shapefile.shapes[25].points);
    }
}

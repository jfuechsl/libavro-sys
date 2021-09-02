#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

// use libc::*;
include!("bindings.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use libc::*;
    use std::ptr::null_mut;
    use std::ffi::{CString, CStr};
    use chrono::Utc;

    #[test]
    fn test_basic() {
        let schema = unsafe {
            let mut schema: avro_schema_t = null_mut();
            let mut err: avro_schema_error_t = null_mut();
            let schema_str = r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
        {"name": "id", "type": "long"}
        ]
            }"#;
            let c_str = CString::new(schema_str).unwrap();
            avro_schema_from_json(c_str.as_ptr() as *const c_char, schema_str.len() as i32, &mut schema, &mut err);
            println!("Error: {:?}", err);
            println!("Type: {:?}", (*schema).type_);
            *schema
        };
        assert_eq!(schema.type_, avro_type_t_AVRO_RECORD);
    }

    unsafe fn avro_value_get_by_name(record: avro_value_t, field_name: &CString) -> (avro_value_t, u64) {
        let mut value: avro_value_t = avro_value_t { self_: null_mut(), iface: null_mut() };
        let mut index: u64 = 0;
        (*record.iface).get_by_name.unwrap()(record.iface, record.self_, field_name.as_ptr(),
                                             &mut value, &mut index);
        (value, index)
    }

    unsafe fn avro_value_get_double(value: avro_value_t) -> f64 {
        let mut val = 0.0;
        (*value.iface).get_double.unwrap()(value.iface, value.self_, &mut val);
        val
    }

    #[test]
    fn test_file_reader() {
        unsafe {
            let filename = CString::new("test.avro").unwrap();
            let mut reader: avro_file_reader_t = null_mut();
            if avro_file_reader(filename.as_ptr(), &mut reader) != 0 {
                let errstr = CStr::from_ptr(avro_strerror());
                println!("{}", errstr.to_str().unwrap());
                panic!("Failed to create avro file reader");
            }
            println!("Wow, all good: {:?}", reader);

            let record_class = avro_generic_class_from_schema(avro_file_reader_get_writer_schema(reader));
            let mut num_records = 0;
            let start = Utc::now();
            loop {
                let mut record: avro_value_t = avro_value_t { iface: null_mut(), self_: null_mut() };
                avro_generic_value_new(record_class, &mut record);
                let rval = avro_file_reader_read_value(reader, &mut record);
                if rval != 0 {
                    let errstr = CStr::from_ptr(avro_strerror());
                    println!("{}", errstr.to_str().unwrap());
                    println!("Read {} before", num_records);
                    if rval == EOF {
                        println!("EOF");
                        break;
                    } else {
                        panic!("Failed to read avro record");
                    }
                } else {
                    num_records += 1;
                    let field_name = CString::new("server_timestamp").unwrap();
                    let (value, _) = avro_value_get_by_name(record, &field_name);
                    let double_val = avro_value_get_double(value);
                    if num_records % 10000 == 0 {
                        println!("Value = {}", double_val);
                    }
                }
            }
            println!("Took: {}", Utc::now() - start);
        }
    }
}
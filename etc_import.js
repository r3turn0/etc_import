const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const csv = require('fast-csv');
const nodemailer = require('nodemailer');
require('dotenv').config();
const DIRECTORY_PATH = process.argv[4]
const CSV_FILE_PATH = path.join(__dirname, DIRECTORY_PATH);
let UUID = '';
let HEADERS = [];
const DATA = [];
const DATE = new Date().toISOString();
const UPLOADED_BY = process.env.UPLOADED_BY;
const STAGING = process.argv[2];
const TABLE = process.argv[3];
const DESTINATION_PATH = process.argv[5];

// Create a new client for PostgreSQL database
const client = new Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_USER_PASSWORD,
    port: process.env.DB_PORT,
});

// Create the mail options
const mailOptions = {
    from: '"' + UPLOADED_BY + '" ' + process.env.SMTP_USER, // Sender address
    to: process.env.SMTP_TO,          // List of recipients
    subject: 'IMPORT: ',              // Subject line
    text: '',                       // Plain text body
    html: ''                 // HTML body
};

const transporter = nodemailer.createTransport({
    host: process.env.SMTP_SERVER,
    port: process.env.SMTP_SERVER_PORT,
    secure: true, // true for port 465, false for other ports
    service: process.env.SMTP_SERVER, // You can use other services like Yahoo, Outlook, etc.
    auth: {
        user: process.env.SMTP_USER, // Your email address
        pass: process.env.SMTP_PASSWORD   // Your email password
    }
});

function sendEmail(mailOptions) {
    mailOptions.html = '<h1>'+mailOptions.subject+'</h1><br><br><br><br>' + mailOptions.html;
    transporter.sendMail(mailOptions, (error, info) => {
        if (error) {
            console.log('Error sending email:', error);
            return console.error('Error sending email:', error);
        }
        console.log('Email sent:', info.response);
    });
}

// Read the CSV file and process the data
async function parseCSV(file) {
    console.log('Parsing csv:', file, 'from' , CSV_FILE_PATH)
    let stream = fs.createReadStream(CSV_FILE_PATH + '\\' + file);
    csv.parseStream(stream, {headers: true})
        .on('error', error => function() { 
            console.error('Error parsing csv stream', error) 
            console.log('Error parsing csv stream', error); 
        })
        .on('headers', (headerList) => {
            headerList.forEach(function(header) {
                HEADERS.push(header.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '').toLowerCase())
            });
            HEADERS.push('filename', 'load_id', 'date_upload', 'uploaded_by');
            console.log('Headers:', HEADERS);
        })
        .on('data', (row) => {
            DATA.push(row);
        })
        .on('end', () => {
            console.log('The Data:', DATA)
            createStagingTable(HEADERS, DATA, file);
        });
}

// A try catch function to create the table if not exists
// else insert the data into the table
async function createStagingTable(headers, data, file) {
    try {
        await client.connect();
        // check to see if the shopify table exists
        const selectQuery = `SELECT to_regclass('`+STAGING+`')`;
        const result = await client.query(selectQuery);
        // If the table already exists truncate then insert the data
        if(result.rows[0].to_regclass) {
            // Truncate the table first
            console.log('Truncating table: ', STAGING);
            await client.query('TRUNCATE ' + STAGING + ' RESTART IDENTITY');
            insertStagingData(headers, data, file);
        }
        else {
            try {
                let columns = headers.map(header => `"${header}" VARCHAR`).join(', ');
                const createQuery = `CREATE TABLE IF NOT EXISTS ` + STAGING + ` (${columns})`;
                console.log('CREATE TABLE QUERY: ', createQuery);
                await client.query(createQuery);
                console.log(STAGING, 'table created successfully');
                //await client.end();
                insertStagingData(headers, data, file);
            } catch (error) {
                await client.end();
                console.error('Error creating table:', STAGING + '. ' + error);
                console.log('Error creating table:', STAGING + '. ' + error);
            }
        }
    }
    catch (error) {
        await client.end();
        console.error('Error checking table:', STAGING + '. ' + error);
        console.log('Error checking table:', STAGING + '. ' + error);
    }
}

// a function to insert data into the database
async function insertStagingData(headers, data, file) {
    try {
        const uuidQuery = `SELECT gen_random_uuid()`;
        const resultUuid = await client.query(uuidQuery);
        const uuid = resultUuid.rows[0].gen_random_uuid;
        UUID = uuid;
        //await client.connect();
        await client.query('BEGIN');
        // inserts row by row
        for (let row of data) {
                row.filename = file;
                row.load_id = uuid;
                row.date_upload = DATE;
                row.uploaded_by = UPLOADED_BY;
                const values = Object.values(row).map((value, index) => `$${index + 1}`).join(', ');
                const rowData = Object.values(row);
                const insertQuery = `INSERT INTO ` + STAGING + ` (${headers.join(', ')}) VALUES (${values})`;
                console.log('The INSERT STAGING QUERY:', insertQuery);
                //console.log('Row Data:', rowData);
                await client.query(insertQuery, rowData);
            }
        await client.query('COMMIT');
        console.log('Data inserted successfully');
        const selectCountQuery = await client.query(`SELECT COUNT(*) AS c FROM ` + STAGING);
        mailOptions.subject = mailOptions.subject + STAGING;
        mailOptions.text = 'Data imported successfully.';
        mailOptions.html = 'Filename: ' + file + '<br><br>' +
                            'Load ID: ' + UUID + '<br><br>' +
                            'Date: ' + DATE + '<br><br>' +
                            'Uploaded By:' + UPLOADED_BY + '<br><br>' + 
                            'Message: ' + mailOptions.text + ' Number of records: ' + selectCountQuery.rows[0].c;
        } catch (error) {
            await client.query('ROLLBACK');
            console.error('Error inserting data to table: ' + STAGING, error);
            console.log('Error inserting data to table: ' + STAGING, error);
            mailOptions.subject = mailOptions.subject + STAGING + ' error';
            mailOptions.text = 'Error importing data: ' + error;
            mailOptions.html = 'Filename: ' + file + '<br><br>' +
                               'Load ID: ' + UUID + '<br><br>' +
                               'Date: ' + DATE + '<br><br>' +
                               'Uploaded By:' + UPLOADED_BY + '<br><br>' +
                                'Message: '+ mailOptions.text;
            await client.end();
        } finally {
            sendEmail(mailOptions);
            // Create Table with field types
            createTable(headers, file);
        }
}

async function getStagingDataTypes() {
    let dataTypes = [];
    try {
        //await client.connect();
        const selectQuery = `SELECT * FROM ` + STAGING + ` LIMIT 1`;
        const results = await client.query(selectQuery);
        if(results.rows.length !== 0) {
            console.log('The Staging Data Types:', results.rows);
            // grab the first row for values 
            const values = Object.values(results.rows[0]);
            for(var i = 0; i < values.length; i++){
                let value = values[i];
                let header = results.fields[i].name;
                if(value !== '' && value !== null && header.match(/name/) === null) {
                    if (!isNaN(value)) {
                        if (Number(value) % 1 === 0) {
                            dataTypes.push('NUMERIC');
                        } else {
                            dataTypes.push('FLOAT');
                        }
                    } else if (!isNaN(Date.parse(value))) {
                        dataTypes.push('TIMESTAMP');
                    } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 't' || value.toLowerCase() === 'false' || value.toLowerCase() === 'f') {
                        dataTypes.push('BOOLEAN');
                    } else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(value)) {
                        dataTypes.push('UUID');
                    } else {
                        dataTypes.push('VARCHAR');
                    }
                } else {
                    dataTypes.push('VARCHAR');
                }
            }
            return dataTypes;
        }
    }
    catch(error) {
        await client.end();
        console.error('Error getting staging data', error);
        console.log('Error getting staging data', error)
    }
}

// Give headers data field types
async function transformHeaders(headers, datatypes){
    // Map headers[] and dataTypes[] to header + ' ' + datatypes
    return headers.map((header, index) => `${header} ${datatypes[index]}`).join(', ');
}

async function createTable(headers, file) {
    try {
        const selectQuery = `SELECT to_regclass('`+TABLE+`')`;
        const result = await client.query(selectQuery);
        if(result.rows[0].to_regclass) {
            insertTableData(headers, file);
        }
        else {
            try {
                const dataTypes = await getStagingDataTypes();
                const columns = await transformHeaders(headers, dataTypes);
                const createQuery = `CREATE TABLE IF NOT EXISTS ` + TABLE + ` (${columns})`;
                console.log('CREATE TABLE QUERY:', createQuery);
                await client.query(createQuery);
                console.log(TABLE, 'table created successfully');
                insertTableData(headers, file);
            } catch (error) {
                await client.end();
                console.error('Error creating table:', TABLE + '. ' + error);
                console.log('Error creating table:', TABLE + '. ' + error);
            }
        }
    }
    catch (error) {
        await client.end();
        console.error('Error checking table:', TABLE + '. ' + error);
        console.log('Error checking table:', TABLE + '. ' + error);
    }
}

async function transformColumns(headers) {
    try {
        //await client.connect();
        // Get the data from staging table
        const selectQuery = `SELECT * FROM ` + STAGING + ` LIMIT 1`;
        const results = await client.query(selectQuery);
        if(results.rows.length !== 0) {
            console.log('The Staging Data:', results.rows);
            const values = Object.values(results.rows[0]);
            for(var i = 0; i < values.length; i++){
                let value = values[i];
                let header = results.fields[i].name;
                if(value !== '' && value !== null && header.match(/name/) === null) {
                    if (!isNaN(value)) {
                        if (Number(value) % 1 === 0) {
                            headers[i] = `CASE WHEN ${headers[i]} = '' OR ${headers[i]} IS NULL THEN 0 ELSE ${headers[i]}::numeric END`;
                        } else {
                            headers[i] = `CASE WHEN ${headers[i]} = '' OR ${headers[i]} IS NULL THEN 0.0 ELSE ${headers[i]}::float END`;
                        }
                    } else if (!isNaN(Date.parse(value))) {
                        headers[i] = `CASE WHEN ${headers[i]} = '' OR ${headers[i]} IS NULL THEN NULL ELSE CAST(${headers[i]} AS TIMESTAMP) END`;
                    } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 't' || value.toLowerCase() === 'false' || value.toLowerCase() === 'f') {
                        headers[i] = `CASE WHEN ${headers[i]} = '' OR ${headers[i]} IS NULL THEN CAST(${headers[i]} AS BOOLEAN) ELSE ${headers[i]}::boolean END`;
                    } else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(value)) {
                        headers[i] = `CAST(${headers[i]} AS UUID)`;
                    } else {
                        headers[i] = headers[i];
                    }
                }
                else {
                    headers[i] = headers[i];
                }
            }
            return headers;
        }
    }
    catch(error) {
        await client.end();
        console.error('Error getting staging data', error);
        console.log('Error getting staging data', error)
    }
}

// a function to insert data into the database
async function insertTableData(headers, file) {
    try {
        //await client.connect();
        await client.query('BEGIN');
        let insertQuery = `INSERT INTO ` + TABLE + ` (${headers.join(', ')}) `;
        const selectedColumns = await transformColumns(headers);
        const selectQuery = `SELECT ${selectedColumns} FROM ${STAGING}`;
        insertQuery += selectQuery;
        console.log('THE INSERT TABLE QUERY:', insertQuery);
        await client.query(insertQuery)
        await client.query('COMMIT');
        console.log('Data inserted successfully');
        const selectTableInfo = await client.query(`SELECT COUNT(*) as c, filename, load_id, date_upload, uploaded_by FROM ` + TABLE + ` where load_id = '` + UUID + `' GROUP BY filename, load_id, date_upload, uploaded_by`);
        const file = selectTableInfo.rows[0].filename;
        const load_id = selectTableInfo.rows[0].load_id;
        const date_upload = selectTableInfo.rows[0].date_upload;
        const uploaded_by = selectTableInfo.rows[0].uploaded_by; 
        const count = selectTableInfo.rows[0].c;
        mailOptions.subject = 'IMPORT: ' + TABLE;
        mailOptions.text = 'Data imported successfully.';
        mailOptions.html = 'Filename: ' + file + '<br><br>' +
                            'Load ID: ' + load_id + '<br><br>' +
                            'Date: ' + date_upload + '<br><br>' +
                            'Uploaded By:' + uploaded_by + '<br><br>' +
                            'Message: ' + mailOptions.text + ' Number of records: ' + count;
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Error inserting data to table: ' + TABLE, error);
        console.log('Error inserting data to table: ' + TABLE, error);
        mailOptions.subject = 'IMPORT: ' + TABLE + ' error';
        mailOptions.text = 'Error importing data: ' + error;
        mailOptions.html = 'Filename: ' + file + '<br><br>' +
                            'Load ID: ' + UUID + '<br><br>' +
                            'Date: ' + DATE + '<br><br>' +
                            'Uploaded By:' + UPLOADED_BY + '<br><br>' +
                            'Message: ' + mailOptions.text;
        await client.end();
    } finally {
        await client.end();
        sendEmail(mailOptions);
    }
}

// Read all files in the directory
// Only files with a .csv extension will be processed
fs.readdir(CSV_FILE_PATH, (err, files) => {
    console.log('Reading csv directory ...');
    if (err) {
        return console.log('Unable to scan directory: ' + err);
    }
    files.forEach((file) => {
        if (path.extname(file) === '.csv') {
            console.log('Processing file: ', file);
            // call the function to process the csv file
            parseCSV(file);
            console.log(file, ' processed');
            fs.copyFile(CSV_FILE_PATH + '\\' + file, DESTINATION_PATH + '\\' + file, function() {
                console.log('File moved from', CSV_FILE_PATH + '\\' + file, 'to', DESTINATION_PATH + '\\' + file);
            });
        }
    });
});
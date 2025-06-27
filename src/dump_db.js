const { exec } = require('child_process');
const path = require('path');

// Configuration
const dbName = 'nirvana';
const uri = "mongodb+srv://<userName>:<passkey>@wh-test-primary.kkhhd.mongodb.net"; // or your MongoDB URI
const outputDir = path.join(__dirname, 'dbDump'); // where dump will be saved

// Build the mongo dump command
const dumpCommand = `mongodump --uri=${uri}/${dbName} --out=${outputDir}`;

// Mongo Restore Local Comment
const localDumpRestoreCommand = `mongorestore --uri='mongodb://localhost:27017/' /Users/Chakkaravarthi/Desktop/Chak/Workhall/BE-DB-DUMP/dump`;

// Build the mongo restore command
const dumpRestoreCommand = `mongorestore --uri=${uri}/${dbName} --dir=${outputDir}`;

// Execute the dump command
exec(dumpCommand, (error) => {
  if (error) {
    console.error(`❌ Error during mongo dump: ${error.message}`);
    return;
  }     

  console.log(`✅ Dump completed successfully! Output located at: ${JSON.stringify(outputDir)}`);
});
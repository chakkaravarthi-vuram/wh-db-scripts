import { s3Utils } from "../core/aws/S3Utils.js";
const devDmsBucketName = 'wh-dev-dms';
// const testDmsBucketName = 'wh-test-dms';
const getListOfObjects = async () => {
    const objects = await s3Utils.listObjects(devDmsBucketName);
    console.log(`Objects fetched successfully from \n ${JSON.stringify(objects)}`);
    return objects;
}

export { getListOfObjects };

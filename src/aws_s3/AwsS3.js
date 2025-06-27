import { s3Utils } from "../core/aws/S3Utils.js";
import { s3BucketName } from "../core/aws/S3.strings.js";

const getListOfObjects = async () => {
    const objects = await s3Utils.listObjects(s3BucketName.WH_DEV_DMS);
    console.log(`Objects fetched successfully from \n ${JSON.stringify(objects)}`);
    return objects;
}

export { getListOfObjects };

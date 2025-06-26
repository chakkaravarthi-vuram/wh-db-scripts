import { s3Client } from "./S3Client.js";
import {
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";

const s3Utils = {
  deleteObject: async (bucketName, key) => {
    const command = new DeleteObjectCommand({
      Bucket: bucketName,
      Key: key,
    });

    try {
      const data = await s3Client.send(command);
      console.log(
        `Object deleted successfully from ${bucketName}: ${key} \n ${JSON.stringify(
          data
        )}`
      );
      return data;
    } catch (error) {
      console.error(`Error deleting object from ${bucketName}: ${key}`, error);
    }
  },
  getObject: async (bucketName, key) => {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key,
    });

    try {
      const data = await s3Client.send(command);
      console.log(`Object fetched successfully from ${bucketName}: ${key}`);
      return data;
    } catch (error) {
      console.error(`Error fetching object from ${bucketName}: ${key}`, error);
    }
  },
  listObjects: async (bucketName) => {
    const command = new ListObjectsV2Command({
      Bucket: bucketName,
    });

    try {
      const data = await s3Client.send(command);
      console.log(`Objects fetched successfully from ${bucketName}`);
      return data.Contents;
    } catch (error) {
      console.error(`Error fetching objects from ${bucketName}`, error);
    }
  },
};

export { s3Utils };

import chalk from "chalk";
import utils from "lodash";
import { closeClient, getClient, getDB } from "../core/Client.js";
import { COLLECTIONS } from "../core/collections/Collections.strings.js";
import { BACKEND_DB } from "../core/backend_db/BackendDB.strings.js";
import { s3Utils } from "../core/aws/S3Utils.js";
import { s3BucketName } from "../core/aws/S3.strings.js";
import { OBJ_UUIDS } from "./Data.js";

const { PRIMARY, WORKHALL_DOCUMENT_ENGINE, SCHEDULER } = COLLECTIONS;

const objIdsAndTechRefName = {
  arrFlowIds: [],
  arrDatalistIds: [],
  arrInstanceIds: [],
  arrJobIds: [],
  arrDocumentIds: [],
  arrTaskIds: [],
  arrDashboardIds: [],
  arrS3Keys: [],
  arrTechnicalReferenceNames: [],
};

const getDatalistIdAndTechnicalReferenceNameListFromDatalist = (datalist) => {
  datalist.forEach((objDatalist) => {
    const { _id, technical_reference_name } = objDatalist;
    objIdsAndTechRefName.arrDatalistIds.push(_id);
    if (
      !utils.includes(
        objIdsAndTechRefName.arrTechnicalReferenceNames,
        technical_reference_name
      )
    ) {
      objIdsAndTechRefName.arrTechnicalReferenceNames.push(
        technical_reference_name
      );
    }
  });
};

const getDatalist = async (db) => {
  const dbCollectionDatalist = await db.collection(PRIMARY.DATA_LIST);
  const dbCollectionDatalistData = await dbCollectionDatalist
    .find({
      data_list_uuid: { $nin: OBJ_UUIDS.arrDatalistUUID },
    })
    .toArray();
  return dbCollectionDatalistData;
};

const getFlowIdAndTechnicalReferenceNameListFromFlowMetaData = (
  flowMetaData
) => {
  flowMetaData.forEach((objFlowMetaData) => {
    const { _id, technical_reference_name } = objFlowMetaData;
    objIdsAndTechRefName.arrFlowIds.push(_id);
    if (
      !utils.includes(
        objIdsAndTechRefName.arrTechnicalReferenceNames,
        technical_reference_name
      )
    ) {
      objIdsAndTechRefName.arrTechnicalReferenceNames.push(
        technical_reference_name
      );
    }
  });
};

const getFlowMetaDataList = async (db) => {
  const dbCollectionFlowMetaData = await db.collection(PRIMARY.FLOW_METADATA);
  const dbCollectionFlowMetaDataData = await dbCollectionFlowMetaData
    .find({
      flow_uuid: { $nin: OBJ_UUIDS.arrFlowUUID },
    })
    .toArray();
  return dbCollectionFlowMetaDataData;
};

const getInstanceIds = (instanceMetaData) => {
  instanceMetaData.forEach((objInstanceMetaData) => {
    const { _id } = objInstanceMetaData;
    objIdsAndTechRefName.arrInstanceIds.push(_id);
  });
};

const getInstanceMetadataList = async (db, query) => {
  const dbCollectionInstanceMetadata = await db.collection(PRIMARY.INSTANCES);
  const dbCollectionInstanceMetadataData = await dbCollectionInstanceMetadata
    .find(query)
    .toArray();
  return dbCollectionInstanceMetadataData;
};

const getDashboardIds = (dashboardMetadata) => {
  dashboardMetadata.forEach((objDashboardMetadata) => {
    const { _id } = objDashboardMetadata;
    if (!utils.includes(objIdsAndTechRefName.arrDashboardIds, _id)) {
      objIdsAndTechRefName.arrDashboardIds.push(_id);
    }
  });
};

const getDashboardMetadataList = async (db, query) => {
  const dbCollectionDashboardMetadata = await db.collection(
    PRIMARY.DASHBOARD_METADATA
  );
  const dbCollectionDashboardMetadataData = await dbCollectionDashboardMetadata
    .find(query)
    .toArray();
  return dbCollectionDashboardMetadataData;
};

const getSchedulerLogList = async (db, schedulerQuery) => {
  const dbCollectionSchedulerLog = await db.collection(PRIMARY.SCHEDULER_LOG);
  const dbCollectionSchedulerLogData = await dbCollectionSchedulerLog
    .find(schedulerQuery)
    .toArray();
  return dbCollectionSchedulerLogData;
};

const getTaskMetaDataList = async (db, query) => {
  const dbCollectionTaskMetadata = await db.collection(PRIMARY.TASK_METADATA);
  const dbCollectionTaskMetadataData = await dbCollectionTaskMetadata
    .find(query)
    .toArray();
  return dbCollectionTaskMetadataData;
};

const getDocumentMetadataList = async (db, documentQuery) => {
  const dbCollectionDocumentMetadata = await db.collection(
    PRIMARY.DOCUMENT_METADATA
  );
  const dbCollectionDocumentMetadataData = await dbCollectionDocumentMetadata
    .find(documentQuery)
    .toArray();
  return dbCollectionDocumentMetadataData;
};

const collectionExists = async (db, collectionName) => {
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();
  return collections.length > 0;
};

const dropCollection = async (db, collectionName) => {
  const dbCollection = await db.collection(collectionName);
  const dbCollectionData = await dbCollection.drop();
  return dbCollectionData;
};

const deleteCollectionsFromObjIdsAndTechRefName = async (db) => {
  let collectionDroppedCount = 0;
  const digitCount = String(objIdsAndTechRefName.arrTechnicalReferenceNames.length).length;
  await objIdsAndTechRefName.arrTechnicalReferenceNames.forEach(
    async (collectionName) => {
      if (await collectionExists(db, collectionName)) {
        const dbCollectionData = await dropCollection(db, collectionName);
        if (dbCollectionData) {
          collectionDroppedCount += 1;
          console.log(
            `\u2714 ${chalk.gray(
              "collection dropped - "
            )}${utils.padStart(String(collectionDroppedCount), digitCount, '0')} ${chalk.redBright.strikethrough.bold(
              collectionName
            )}`
          );
        }
      }
    }
  );
};

const deleteMany = async (db, collectionName, query) => {
  const dbCollectionData = await db
    .collection(collectionName)
    .deleteMany(query);
  if (dbCollectionData.acknowledged) {
    console.log(
      `\u2714 ${chalk.whiteBright.bold(collectionName)} ${chalk.gray(
        "deleted counts = "
      )}`,
      dbCollectionData.deletedCount
    );
  }
};

const deleteCollectionsData = async (db) => {
  // datalist
  const datalistData = await getDatalist(db);
  await getDatalistIdAndTechnicalReferenceNameListFromDatalist(datalistData);

  // flow_metadata
  const flowMetaData = await getFlowMetaDataList(db);
  await getFlowIdAndTechnicalReferenceNameListFromFlowMetaData(flowMetaData);

  const query = {
    $and: [
      { data_list_uuid: { $nin: OBJ_UUIDS.arrDatalistUUID } },
      { flow_uuid: { $nin: OBJ_UUIDS.arrFlowUUID } },
    ],
  };

  // instance
  const instanceMetadata = await getInstanceMetadataList(db, query);
  await getInstanceIds(instanceMetadata);

  // dashboard_metadata
  const dashboardMetadata = await getDashboardMetadataList(db, query);
  await getDashboardIds(dashboardMetadata);

  let UUIDs = [];
  UUIDs.push(OBJ_UUIDS.arrDatalistUUID);
  UUIDs.push(OBJ_UUIDS.arrFlowUUID);
  // UUIDs.push(OBJ_UUIDS.arrTaskMetadataUuids);
  UUIDs = UUIDs.flat(Infinity);

  // Delete collections
  await deleteCollectionsFromObjIdsAndTechRefName(db);

  // Delete Many //

  // Delete Many action_history
  await deleteMany(db, PRIMARY.ACTION_HISTORY, query);

  // Delete Many field_master
  const fieldMasterQuery = {
    context_uuid: { $nin: UUIDs },
  };
  await deleteMany(db, PRIMARY.FIELD_MASTER, fieldMasterQuery);

  // Delete Many instance
  await deleteMany(db, PRIMARY.INSTANCES, query);

  // Delete Many dashboard_metadata
  await deleteMany(db, PRIMARY.DASHBOARD_METADATA, query);

  // Delete Many dashboard_pages
  const dashboardPagesQuery = {
    dashboard_id: { $in: objIdsAndTechRefName.arrDashboardIds },
  };
  await deleteMany(db, PRIMARY.DASHBOARD_PAGES, dashboardPagesQuery);

  // Delete Many dashboard_components
  await deleteMany(db, PRIMARY.DASHBOARD_COMPONENTS, dashboardPagesQuery);

  // Delete Many active_tasks
  await deleteMany(db, PRIMARY.ACTIVE_TASKS, query);

  // Delete Many field_metadata
  await deleteMany(db, PRIMARY.FIELD_METADATA, query);

  // Delete Many active_task_details
  // const activeTaskDetailsQuery = {
  //   instance_id: { $in: objIdsAndTechRefName.arrInstanceIds },
  // };
  // await deleteMany(db, PRIMARY.ACTIVE_TASK_DETAILS, activeTaskDetailsQuery);

  // Delete Many task_log
  await deleteMany(db, PRIMARY.TASK_LOG, query);

  const schedulerQuery = {
    $and: [
      { "schedule_data.flow_uuid": { $nin: OBJ_UUIDS.arrFlowUUID } },
      { "schedule_data.data_list_uuid": { $nin: OBJ_UUIDS.arrDatalistUUID } },
      // {
      //   "schedule_data.instance_id": {
      //     $in: objIdsAndTechRefName.arrInstanceIds,
      //   },
      // },
    ],
  };

  // Get scheduler_log list
  const schedulerLogList = await getSchedulerLogList(db, schedulerQuery);
  objIdsAndTechRefName.arrJobIds = schedulerLogList.map((item) => item.job_id);

  // Get Task Metadata List
  const taskMetadataList = await getTaskMetaDataList(db, query);
  objIdsAndTechRefName.arrTaskIds = taskMetadataList.map((item) => item._id);

  // Delete Many scheduler_log
  await deleteMany(db, PRIMARY.SCHEDULER_LOG, schedulerQuery);

  // Delete Many data_access_log
  const dataAccessLogQuery = {
    data_access_log_uuid: { $nin: UUIDs },
  };
  await deleteMany(db, PRIMARY.DATA_ACCESS_LOG, dataAccessLogQuery);

  // Delete Many task_owner_log
  await deleteMany(db, PRIMARY.TASK_OWNER_LOG, query);

  // Delete Many flow_steps
  await deleteMany(db, PRIMARY.FLOW_STEPS, query);

  // Delete Many auto_sequence
  const autoSequenceQuery = {
    $and: [{ identifier: { $nin: UUIDs } }],
  };
  await deleteMany(db, PRIMARY.AUTO_SEQUENCE, autoSequenceQuery);

  // Delete Many flow_metadata
  await deleteMany(db, PRIMARY.FLOW_METADATA, query);

  // Delete Many data_list
  await deleteMany(db, PRIMARY.DATA_LIST, query);

  // Delete Many rule_metadata
  const ruleMetaDataQuery = {
    $or: [
      { data_list_id: { $in: objIdsAndTechRefName.arrDatalistIds } },
      { flow_id: { $in: objIdsAndTechRefName.arrFlowIds } },
      { task_metadata_id: { $in: objIdsAndTechRefName.arrTaskIds } },
    ],
  };
  await deleteMany(db, PRIMARY.RULE_METADATA, ruleMetaDataQuery);

  // Delete Many trigger_metadata
  const triggerMetaDataQuery = {
    $or: [
      { data_list_id: { $in: objIdsAndTechRefName.arrDatalistIds } },
      { flow_id: { $in: objIdsAndTechRefName.arrFlowIds } },
    ],
  };
  await deleteMany(db, PRIMARY.TRIGGER_METADATA, triggerMetaDataQuery);

  // Delete Many task_metadata
  // await deleteMany(db, PRIMARY.TASK_METADATA, query);

  // Delete Many documentQuery
  const documentQuery = {
    $and: [
      { entity_uuid: { $nin: UUIDs } },
      // { entity_id: { $in: objIdsAndTechRefName.arrTaskIds } },
    ],
  };

  // Get document_metadata list
  const documentMetadataList = await getDocumentMetadataList(db, documentQuery);
  objIdsAndTechRefName.arrDocumentIds = documentMetadataList.map(
    (item) => item._id
  );

  // Delete Many document_metadata
  await deleteMany(db, PRIMARY.DOCUMENT_METADATA, documentQuery);

  // Delete Many apps
  const appsQuery = { app_uuid: { $nin: OBJ_UUIDS.arrAppUUID } };
  await deleteMany(db, PRIMARY.APPS, appsQuery);

  // Delete Many pages
  await deleteMany(db, PRIMARY.PAGES, appsQuery);

  // Delete Many components
  await deleteMany(db, PRIMARY.COMPONENTS, appsQuery);

  // Delete Many aggregate_report_metadata
  const aggregateReportMetadataQuery = {
    report_uuid: { $nin: OBJ_UUIDS.arrReportUUID },
  };
  await deleteMany(
    db,
    PRIMARY.AGGREGATE_REPORT_METADATA,
    aggregateReportMetadataQuery
  );
};

const getDocumentLogList = async (db, query) => {
  const documentLogList = await db
    .collection(WORKHALL_DOCUMENT_ENGINE.DOCUMENT_LOG)
    .find(query)
    .toArray();
  return documentLogList;
};

const deleteCollectionDataFromWorkhallDocumentEngineDB = async (client) => {
  const dbWHDocumentEngine = await getDB(
    client,
    BACKEND_DB.WORKHALL_DOCUMENT_ENGINE
  );

  const documentLogQuery = {
    document_id: { $in: objIdsAndTechRefName.arrDocumentIds },
  };

  // Get document_log list
  const documentLogList = await getDocumentLogList(
    dbWHDocumentEngine,
    documentLogQuery
  );
  objIdsAndTechRefName.arrS3Keys = documentLogList.map((item) => item.s3_key);

  // Delete S3 Objects
  objIdsAndTechRefName.arrS3Keys.forEach(async (s3Key) => {
    await s3Utils.deleteObject(s3BucketName.WH_DEV_DMS, s3Key);
  });

  // Delete Many document_log
  await deleteMany(
    dbWHDocumentEngine,
    WORKHALL_DOCUMENT_ENGINE.DOCUMENT_LOG,
    documentLogQuery
  );
};

const deleteCollectionDataFromSchedulerDB = async (client) => {
  const dbScheduler = await getDB(client, BACKEND_DB.SCHEDULER);

  const query = {
    job_id: { $in: objIdsAndTechRefName.arrJobIds },
  };

  // Delete Many scheduler_audit
  await deleteMany(dbScheduler, SCHEDULER.SCHEDULER_AUDIT, query);

  // Delete Many recursive_jobs
  await deleteMany(dbScheduler, SCHEDULER.RECURSIVE_JOBS, query);
};

const deleteCollections = async () => {
  try {
    const client = await getClient();

    const db = await getDB(client, BACKEND_DB.INTERNAL);
    await deleteCollectionsData(db);

    await deleteCollectionDataFromWorkhallDocumentEngineDB(client);
    await deleteCollectionDataFromSchedulerDB(client);

    await closeClient(client);
  } catch (error) {
    console.error(error);
  }
};

export { deleteCollections };

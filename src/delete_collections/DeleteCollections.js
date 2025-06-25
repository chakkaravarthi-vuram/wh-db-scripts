import { closeClient, getClient, getDB } from "../core/Client.js";
import { COLLECTIONS } from "../core/collections/Collections.strings.js";
import { BACKEND_DB } from "../core/backend_db/BackendDB.strings.js";
import utils from "lodash";

// We have to put the all UUID's
const objUuids = {
  arrDataListUuids: [
    "407461af-611b-4dfd-a748-bab24d679030",
    "dcb8fc3a-a614-4f41-81a2-1a8c10b7a782",
  ],
  arrFlowUuids: ["0e2cb1ba-6635-43a2-be82-8d9c7e1bb031"],
};

const objIdsAndTechRefName = {
  arrFlowIds: [],
  arrDatalistIds: [],
  arrInstanceIds: [],
  arrJobIds: [],
  arrTaskIds: [],
  arrDashboardIds: [],
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
  const dbCollectionDatalist = await db.collection(COLLECTIONS.DATA_LIST);
  const dbCollectionDatalistData = await dbCollectionDatalist
    .find({
      data_list_uuid: { $nin: objUuids.arrDataListUuids },
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
  const dbCollectionFlowMetaData = await db.collection(
    COLLECTIONS.FLOW_METADATA
  );
  const dbCollectionFlowMetaDataData = await dbCollectionFlowMetaData
    .find({
      flow_uuid: { $nin: objUuids.arrFlowUuids },
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
  const dbCollectionInstanceMetadata = await db.collection(
    COLLECTIONS.INSTANCES
  );
  const dbCollectionInstanceMetadataData = await dbCollectionInstanceMetadata
    .find(query)
    .toArray();
  return dbCollectionInstanceMetadataData;
};

const getDashboardIds = (dashboardMetadata) => {
  dashboardMetadata.forEach((objDashboardMetadata) => {
    const { _id } = objDashboardMetadata;
    objIdsAndTechRefName.arrDashboardIds.push(_id);
  });
};

const getDashboardMetadataList = async (db, query) => {
  const dbCollectionDashboardMetadata = await db.collection(
    COLLECTIONS.DASHBOARD_METADATA
  );
  const dbCollectionDashboardMetadataData = await dbCollectionDashboardMetadata
    .find(query)
    .toArray();
  return dbCollectionDashboardMetadataData;
};

const getSchedulerLogList = async (db, schedulerQuery) => {
  const dbCollectionSchedulerLog = await db.collection(
    COLLECTIONS.SCHEDULER_LOG
  );
  const dbCollectionSchedulerLogData = await dbCollectionSchedulerLog
    .find(schedulerQuery)
    .toArray();
  return dbCollectionSchedulerLogData;
};

const getTaskMetaDataList = async (db, query) => {
  const dbCollectionTaskMetadata = await db.collection(
    COLLECTIONS.TASK_METADATA
  );
  const dbCollectionTaskMetadataData = await dbCollectionTaskMetadata
    .find(query)
    .toArray();
  return dbCollectionTaskMetadataData;
};

const getDocumentMetadataList = async (db, documentQuery) => {
  const dbCollectionDocumentMetadata = await db.collection(
    COLLECTIONS.DOCUMENT_METADATA
  );
  const dbCollectionDocumentMetadataData = await dbCollectionDocumentMetadata
    .find(documentQuery)
    .toArray();
  return dbCollectionDocumentMetadataData;
};

const dropCollection = async (db, collectionName) => {
  const dbCollection = await db.collection(collectionName);
  const dbCollectionData = await dbCollection.drop();
  return dbCollectionData;
};

const deleteCollectionsFromObjIdsAndTechRefName = async (db) => {
  objIdsAndTechRefName.arrTechnicalReferenceNames.forEach(
    async (collectionName) => {
      const dbCollectionData = await dropCollection(db, collectionName);
      if (dbCollectionData) {
        console.log("collection dropped -> ", collectionName);
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
      `${collectionName} deleted counts = `,
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

  // instance
  const instanceMetadata = await getInstanceMetadataList(db);
  await getInstanceIds(instanceMetadata);

  // dashboard_metadata
  const dashboardMetadata = await getDashboardMetadataList(db);
  await getDashboardIds(dashboardMetadata);

  const query = {
    $and: [
      { data_list_uuid: { $nin: objUuids.arrDataListUuids } },
      { flow_uuid: { $nin: objUuids.arrFlowUuids } },
    ],
  };
  let docUuids = [];
  docUuids.push(objUuids.arrDataListUuids);
  docUuids.push(objUuids.arrFlowUuids);
  // docUuids.push(objUuids.arrTaskMetadataUuids);
  docUuids = docUuids.flat(Infinity);

  // Delete collections
  await deleteCollectionsFromObjIdsAndTechRefName(db);

  // Delete Many //

  // Delete Many action_history
  await deleteMany(db, COLLECTIONS.ACTION_HISTORY, query);

  // Delete Many field_master
  const fieldMasterQuery = {
    context_uuid: { $nin: docUuids },
  };
  await deleteMany(db, COLLECTIONS.FIELD_MASTER, fieldMasterQuery);

  // Delete Many instance
  await deleteMany(db, COLLECTIONS.INSTANCES, query);

  // Delete Many dashboard_metadata
  await deleteMany(db, COLLECTIONS.DASHBOARD_METADATA, query);

  // Delete Many dashboard_pages
  const dashboardPagesQuery = {
    dashboard_id: { $in: objIdsAndTechRefName.arrDashboardIds },
  };
  await deleteMany(db, COLLECTIONS.DASHBOARD_PAGES, dashboardPagesQuery);

  // Delete Many dashboard_components
  await deleteMany(db, COLLECTIONS.DASHBOARD_COMPONENTS, dashboardPagesQuery);

  // Delete Many active_tasks
  await deleteMany(db, COLLECTIONS.ACTIVE_TASKS, query);

  // Delete Many field_metadata
  await deleteMany(db, COLLECTIONS.FIELD_METADATA, query);

  // Delete Many active_task_details
  const activeTaskDetailsQuery = {
    instance_id: { $in: objIdsAndTechRefName.arrInstanceIds },
  };
  await deleteMany(db, COLLECTIONS.ACTIVE_TASK_DETAILS, activeTaskDetailsQuery);

  // Delete Many task_log
  await deleteMany(db, COLLECTIONS.TASK_LOG, query);

  const schedulerQuery = {
    $and: [
      { "schedule_data.flow_uuid": { $nin: objUuids.arrFlowUuids } },
      { "schedule_data.data_list_uuid": { $nin: objUuids.arrDataListUuids } },
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
  await deleteMany(db, COLLECTIONS.SCHEDULER_LOG, schedulerQuery);

  // Delete Many data_access_log
  const dataAccessLogQuery = {
    data_access_log_uuid: { $nin: docUuids },
  };
  await deleteMany(db, COLLECTIONS.DATA_ACCESS_LOG, dataAccessLogQuery);

  // Delete Many task_owner_log
  await deleteMany(db, COLLECTIONS.TASK_OWNER_LOG, query);

  // Delete Many flow_steps
  await deleteMany(db, COLLECTIONS.FLOW_STEPS, query);

  // Delete Many auto_sequence
  const autoSequenceQuery = {
    $and: [{ identifier: { $nin: docUuids } }],
  };
  await deleteMany(db, COLLECTIONS.AUTO_SEQUENCE, autoSequenceQuery);

  // Delete Many flow_metadata
  await deleteMany(db, COLLECTIONS.FLOW_METADATA, query);

  // Delete Many data_list
  await deleteMany(db, COLLECTIONS.DATA_LIST, query);

  // Delete Many rule_metadata
  const ruleMetaDataQuery = {
    $or: [
      { data_list_id: { $in: objIdsAndTechRefName.arrDatalistIds } },
      { flow_id: { $in: objIdsAndTechRefName.arrFlowIds } },
      { task_metadata_id: { $in: objIdsAndTechRefName.arrTaskIds } },
    ],
  };
  await deleteMany(db, COLLECTIONS.RULE_METADATA, ruleMetaDataQuery);

  // Delete Many trigger_metadata
  const triggerMetaDataQuery = {
    $or: [
      { data_list_id: { $in: objIdsAndTechRefName.arrDatalistIds } },
      { flow_id: { $in: objIdsAndTechRefName.arrFlowIds } },
    ],
  };
  await deleteMany(db, COLLECTIONS.TRIGGER_METADATA, triggerMetaDataQuery);

  // Delete Many task_metadata
  await deleteMany(db, COLLECTIONS.TASK_METADATA, query);

  // Delete Many documentQuery
  const documentQuery = {
    $and: [
      { entity_uuid: { $nin: docUuids } },
      // { entity_id: { $in: objIdsAndTechRefName.arrTaskIds } },
    ],
  };

  // Get document_metadata list
  const documentMetadataList = await getDocumentMetadataList(db, documentQuery);
  objIdsAndTechRefName.arrDocumentIds = documentMetadataList.map(
    (item) => item._id
  );

  // Delete Many document_metadata
  await deleteMany(db, COLLECTIONS.DOCUMENT_METADATA, documentQuery);

  return true;
};

const deleteCollections = async () => {
  try {
    const client = await getClient(true); // false for server

    const db = await getDB(client, BACKEND_DB.NIRVANA);
    await deleteCollectionsData(db);

    await closeClient(client);
  } catch (error) {
    console.error(error);
  }
};

export { deleteCollections };

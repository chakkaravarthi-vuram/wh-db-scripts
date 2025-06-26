import { deleteCollections } from './delete_collections/DeleteCollections.js';
const execute = async () => {
  try {
    await deleteCollections();
  } catch (error) {
    console.error(error);
  }
};
execute();

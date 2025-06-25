import { deleteCollections } from './delete_collections/index.js';
const execute = async () => {
  try {
    await deleteCollections();
  } catch (error) {
    console.error(error);
  }
};
execute();

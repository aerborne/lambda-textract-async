import { TextractClient, GetDocumentAnalysisCommand } from "@aws-sdk/client-textract";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const textract = new TextractClient({ region: process.env.CDK_DEFAULT_REGION });
const s3 = new S3Client({ region: process.env.CDK_DEFAULT_REGION });


function find_value_block(key_block, value_map) {
  let value_block = ""
  key_block["Relationships"].forEach(relationship => {
    if (relationship["Type"] == "VALUE") {
      relationship["Ids"].forEach(value_id => {
        value_block = value_map[value_id]
      })
    }
  })
  return value_block
}

function get_text(result, blocks_map) {
  let text = ""
  let word
  if (result["Relationships"]) {
    result["Relationships"].forEach(relationship => {
      if (relationship["Type"] === "CHILD") {
        relationship["Ids"].forEach(child_id => {
          word = blocks_map[child_id]

          if (word["BlockType"] == "WORD") {
            text += word["Text"] + " "
          }
          if (word["BlockType"] == "SELECTION_ELEMENT") {
            if (word["SelectionStatus"] == "SELECTED") {
              text += "X "
            }
          }
        })
      }
    })
  }
  return text
}

function getKvMap(resp) {
  // get key and value maps
  let key_map = {}
  let value_map = {}
  let block_map = {}

  resp["Blocks"].forEach(block => {
    const block_id = block["Id"]
    block_map[block_id] = block
    if (block["BlockType"] == "KEY_VALUE_SET") {
      if (block["EntityTypes"].includes("KEY")) {
        key_map[block_id] = block
      } else {
        value_map[block_id] = block
      }
    }
  })

  return [key_map, value_map, block_map]
}


function getKvRelationship(keyMap, valueMap, blockMap) {
  let kvs = {}
  Object.keys(keyMap).forEach(blockId => {
    const keyBlock = keyMap[blockId]
    const value_block = find_value_block(keyBlock, valueMap)

    const key = get_text(keyBlock, blockMap)
    const val = get_text(value_block, blockMap)
    kvs[key] = val
  })

  return kvs
}

export const handler = async (event = {}) => {
  console.log("job start")
  console.log(JSON.stringify(event, null, 4))

  let JobId = ""

  // get jobId from event
  try {
    JobId = event["Records"][0]["Sns"]["Message"]
    const jobIDStruct = JSON.parse(JobId)
    JobId = jobIDStruct["JobId"]
  } catch (e) {
    // Logs error message from
    console.log("Error parsing JobId from SNS message")
    console.log(e)
    return
  }

  console.log({ JobId })

  const getDocumentAnalysisParams = {
    JobId
  }

  console.log("getDocumentAnalysisParams")
  console.log(getDocumentAnalysisParams)

  // Fires off textract getDocumentAnalysis
  const command = new GetDocumentAnalysisCommand(getDocumentAnalysisParams);
  const data = await textract.send(command);
  console.log(data)

  // Gets KV mapping
  const [keyMap, valueMap, blockMap] = getKvMap(data)

  // Get Key Value relationship
  const kvPairs = getKvRelationship(keyMap, valueMap, blockMap)

  // Logs form key-value pairs from Textract response
  console.log("Got KV pairs")

  // Sanitize KV pairs
  const sanitizedKvPairs = {}

  // Iterate over each key in kvPairs
  Object.keys(kvPairs).forEach(key => {
    // Sanitizes the key from kv pairs
    const sanitizedKey = key
      .toLowerCase()
      .trim()
      .replace(/\s/g, "_")
      .replace(":", "")

    // Pulls value from kbPairs, trims whitespace
    const value = kvPairs[key].trim()

    // Assigns value from kvPairs to sanitizedKey
    if (value !== "") {
      sanitizedKvPairs[sanitizedKey] = kvPairs[key]
    }
  })

  console.log({sanitizedKvPairs})

  // upload to s3
  const input = {
    Bucket: process.env.AWS_S3_BUCKET,
    Key: `${JobId}.json`,
    Body: JSON.stringify(sanitizedKvPairs),
    ContentType: 'application/json; charset=utf-8'
  }

  const putObject = new PutObjectCommand(input);
  const response = await s3.send(putObject);

  console.log(response)
  console.log("job end")
}

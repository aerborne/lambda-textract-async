import { TextractClient, StartDocumentAnalysisCommand } from "@aws-sdk/client-textract";

const textract = new TextractClient({ region: process.env.CDK_DEFAULT_REGION });
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN || "";
const SNS_ROLE_ARN = process.env.SNS_ROLE_ARN || "";
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME || "";

export const handler = async (event = {}) => {
  console.log("job start");
  console.log(JSON.stringify(event, null, 4));

  const filename = event["Records"][0]["s3"]["object"]["key"];

  if (!filename) {
    console.log("ERROR - no filename for s3 object");
    return;
  }

  const params = {
    DocumentLocation: {
      S3Object: {
        Bucket: S3_BUCKET_NAME,
        Name: filename,
      },
    },
    FeatureTypes: ["FORMS"],
    NotificationChannel: {
      RoleArn: SNS_ROLE_ARN,
      SNSTopicArn: SNS_TOPIC_ARN,
    },
  };

  console.log("startDocumentAnalysis params");
  console.log(params);

  // Invoke Textract.startDocumentAnalysis
  const command = new StartDocumentAnalysisCommand(params);
  const response = await textract.send(command);
  // Logs success state
  console.log("startDocumentAnalysis - response");
  console.log(response);

  console.log("job end");
  return;
};
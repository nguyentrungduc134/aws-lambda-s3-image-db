# Lambda Function for S3 Image Processing and Database Management

## Overview

This Lambda function is designed to process images uploaded to an S3 bucket. It performs the following tasks:

1. **Image Resizing**: When an image is uploaded, it is resized to a fixed width while maintaining the aspect ratio.
2. **Metadata Extraction**: The function retrieves metadata from the image using a child Lambda function.
3. **Database Operations**: It interacts with a MySQL database to insert, update, and delete records related to the images and folders.

## Prerequisites

- **AWS S3**: The function listens to S3 events and processes images stored in an S3 bucket.
- **AWS Lambda**: The function is deployed as an AWS Lambda function.
- **MySQL Database**: The function connects to a MySQL database to manage image and folder records.
- **Python Libraries**:
  - `pymysql`
  - `boto3`
  - `cv2` (OpenCV)
  - `numpy`

## Environment Variables

- `REGION`: The AWS region where the resources are located (e.g., `eu-central-1`).
- `HCLOUD_TOKEN`: Required for HCloud snapshot creation (if applicable).
- `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`: MySQL database connection details.

## Deployment

### 1. Install Dependencies

Before deploying the Lambda function, ensure the required Python libraries are installed and packaged with the function.

### 2. Create a Lambda Function

- Use the AWS Management Console or AWS CLI to create a Lambda function.
- Upload the deployment package (including dependencies).

### 3. Configure the S3 Bucket

- Configure the S3 bucket to trigger the Lambda function on object creation and deletion events.

### 4. Set Environment Variables

- Set the necessary environment variables in the Lambda function configuration.

## Usage

### Image Upload

When an image is uploaded to the S3 bucket, the function:

1. Resizes the image.
2. Stores the resized image in a `resized/` subfolder in the same bucket.
3. Inserts a record into the MySQL database with details of the image and its resized version.
4. Retrieves metadata using a child Lambda function and updates the database record.

### Folder Creation/Deletion

- **Folder Creation**: When a folder is created in S3, it is recorded in the database.
- **Folder Deletion**: When a folder is deleted, the function removes the corresponding database records.

### Image Deletion

- When an image is deleted from S3, the function removes the corresponding record from the database.

## Cleanup

The Lambda function ensures the MySQL database connection is closed properly after each invocation.

## Example Event

Here is an example S3 event that the function can process:

```json
{
  "Records": [
    {
      "s3": {
        "bucket": {
          "name": "your-bucket-name"
        },
        "object": {
          "key": "path/to/your/image.jpg"
        }
      },
      "eventName": "ObjectCreated:Put"
    }
  ]
}
```

## Error Handling

The function includes error handling to manage potential issues such as failed database operations or S3 object manipulation errors. In case of an error, the function returns a 500 status code with the error message.

## Testing

You can test the function by uploading and deleting images or folders in the configured S3 bucket and verifying the database entries.

---

This README provides a comprehensive guide to deploying, configuring, and using the Lambda function for image processing and database management.

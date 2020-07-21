/**
 * Copyright 2016, Google, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"use strict";
// a configuration file defining the pubsub topics and BigQuery details
const config = require("./config.json");

const Buffer = require("safe-buffer").Buffer;

const videoSafeSearchMap = [
  "UNKNOWN",
  "VERY_UNLIKELY",
  "UNLIKELY",
  "POSSIBLE",
  "LIKELY",
  "VERY_LIKELY",
];

// [START functions_publishResult]
/**
 * Publishes the result to the given pubsub topic and returns a Promise.
 *
 * @param {string} topicName Name of the topic on which to publish.
 * @param {object} data The message data to publish.
 */
function publishResult(topicName, data) {
  const { PubSub } = require("@google-cloud/pubsub");
  const pubsub = new PubSub();
  var buf = Buffer.from(JSON.stringify(data));
  return pubsub
    .topic(topicName)
    .get({ autoCreate: true })
    .then(([topic]) => topic.publish(buf));
}
// [END functions_publishResult]

// [START functions_GCStoPubsub]
/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event which contains a pubsub message
 */
exports.GCStoPubsub = function GCStoPubsub(event) {
  const eventData = event.data;
  const jsonData = Buffer.from(eventData, "base64").toString();

  var jsonObj = JSON.parse(jsonData);

  return Promise.resolve()
    .then(() => {
      if (typeof jsonObj.bucket === "undefined" || !jsonObj.bucket) {
        console.error(`Input request: ${jsonData}`);
        throw new Error(
          'Bucket not provided. Make sure you have a "bucket" property in your request'
        );
      }
      if (typeof jsonObj.name === "undefined" || !jsonObj.name) {
        console.error(`Input request: ${jsonData}`);
        throw new Error(
          'Filename not provided. Make sure you have a "name" property in your request'
        );
      }
      if (typeof jsonObj.contentType === "undefined" || !jsonObj.contentType) {
        console.error(`Input request: ${jsonData}`);
        throw new Error(
          'ContentType not provided. Make sure you have a "contentType" property in your request'
        );
      }
      if (
        jsonObj.contentType.search(/video/i) == -1 &&
        jsonObj.contentType.search(/image/i) == -1
      ) {
        console.error(`Input request: ${jsonData}`);
        throw new Error(
          'Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request'
        );
      }

      console.log(
        `Received name: ${jsonObj.name} and bucket: ${jsonObj.bucket} and contentType: ${jsonObj.contentType}`
      );

      //move the current file to the results bucket
      return moveFile(
        jsonObj.bucket,
        jsonObj.name,
        config.RESULT_BUCKET,
        jsonObj.name
      ).then(() => {
        console.info("Completed file move");
      });
    })
    .then(() => {
      // build a msg for pubsub
      const msgData = {
        contentType: jsonObj.contentType,
        gcsUrl: "gs://" + config.RESULT_BUCKET + "/" + jsonObj.name,
        gcsBucket: config.RESULT_BUCKET,
        gcsFile: jsonObj.name,
      };

      if (jsonObj.contentType) {
        // if we have an image, call the Vision API
        // if we have a video, call the Video Intelligence API
        if (jsonObj.contentType.search(/image/i) > -1) {
          console.info(`Vision API request ${JSON.stringify(msgData)}`);
          console.log(`Sending Vision API request`);
          return publishResult(config.VISION_TOPIC, msgData);
        } else if (jsonObj.contentType.search(/video/i) > -1) {
          console.info(
            `Sending Video Intelligence API request ${JSON.stringify(msgData)}`
          );
          console.log(`Sending Video Intelligence API request`);
          return publishResult(config.VIDEOINTELLIGENCE_TOPIC, msgData);
        } else {
          console.error(
            "Incorrect file type: Received contentType " +
              jsonObj.contentType +
              " which is not an image or video file"
          );
          throw new Error(
            'Unsupported ContentType provided. Make sure you include a "contentType" property of image or video in your request'
          );
        }
      } else {
        console.error(
          "No file type: Received contentType " +
            jsonObj.contentType +
            " which is not an image or video file"
        );
        throw new Error(
          'ContentType not provided. Make sure you have a "contentType" property in your request'
        );
      }
    })
    .then(() => {
      console.log(`File ${jsonObj.name} processed by image or video API.`);
    });
};
// [END functions_GCStoPubsub]

// [START functions_insertIntoBigQuery]
/**
 * Function called with a request to insert a row into BigQuery
 *
 * @param {object} event The Cloud Functions event which contains a BigQuery insert request object specifying 1 row
 */

exports.insertIntoBigQuery = function insertIntoBigQuery(event) {
  console.log("Enter bqinsert function");

  // Get a reference to the BigQuery API component
  const { BigQuery } = require("@google-cloud/bigquery");
  const bigquery = new BigQuery();

  const reqData = Buffer.from(event.data, "base64").toString();
  const reqDataObj = JSON.parse(reqData);
  console.info(reqDataObj);

  return Promise.resolve()
    .then(() => {
      if (typeof reqDataObj.gcsUrl === "undefined" || !reqDataObj.gcsUrl) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'GCSUrl not provided. Make sure you have a "gcsUrl" property in your request'
        );
      }

      if (
        typeof reqDataObj.contentUrl === "undefined" ||
        !reqDataObj.contentUrl
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'ContentUrl not provided. Make sure you have a "contentUrl" property in your request'
        );
      }
      if (
        typeof reqDataObj.contentType === "undefined" ||
        !reqDataObj.contentType
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'ContentType not provided. Make sure you have a "contentType" property in your request'
        );
      }
      if (
        reqDataObj.contentType.search(/video/i) == -1 &&
        reqDataObj.contentType.search(/image/i) == -1
      ) {
        console.error(
          `Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request`
        );
        throw new Error(
          'Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request'
        );
      }
      if (
        typeof reqDataObj.insertTimestamp === "undefined" ||
        !reqDataObj.insertTimestamp
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'insertTimestamp not provided. Make sure you have a "insertTimestamp" property in your request'
        );
      }

      console.log(`Sending BigQuery insert request`);
      const bqDataset = bigquery.dataset(config.DATASET_ID);
      const bqTable = bqDataset.table(config.TABLE_NAME);

      return bqTable.insert(reqDataObj);
    })
    .then(function (data) {
      const apiResponse = data[0];
      console.log("Inserted the record into BigQuery");
      console.info(apiResponse);
    })
    .then(() => {
      console.log(`Insert request complete`);
    });
};
// [END functions_insertIntoBigQuery]

// [START functions_visionAPI]
/**
 * Function to run a file through the Vision API and insert the results in BigQuery
 *
 * @param {object} event The Cloud Functions event which contains a message with the GCS file details
 */
exports.visionAPI = function visionAPI(event) {
  console.log("Start visionAPI function");

  // Imports the Google Cloud client library
  const vision_api = require("@google-cloud/vision");

  // Creates a client
  const vision = new vision_api.ImageAnnotatorClient();

  const reqData = Buffer.from(event.data, "base64").toString();
  const reqDataObj = JSON.parse(reqData);
  console.info(reqData);
  var bqInsertObj = {};

  return Promise.resolve()
    .then(() => {
      if (
        typeof reqDataObj.gcsBucket === "undefined" ||
        !reqDataObj.gcsBucket
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'Bucket not provided. Make sure you have a "gcsBucket" property in your request'
        );
      }
      if (typeof reqDataObj.gcsFile === "undefined" || !reqDataObj.gcsFile) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'Filename not provided. Make sure you have a "gcsFile" property in your request'
        );
      }
      if (
        typeof reqDataObj.contentType === "undefined" ||
        !reqDataObj.contentType
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'ContentType not provided. Make sure you have a "contentType" property in your request'
        );
      }
      if (typeof reqDataObj.gcsUrl === "undefined" || !reqDataObj.gcsUrl) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'GCS URL not provided. Make sure you have a "gcsUrl" property in your request'
        );
      }
      if (
        reqDataObj.contentType.search(/video/i) == -1 &&
        reqDataObj.contentType.search(/image/i) == -1
      ) {
        console.error(
          `Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request`
        );
        throw new Error(
          'Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request'
        );
      }

      console.log(
        `Received name: ${reqDataObj.gcsFile} and bucket: ${reqDataObj.gcsBucket} and contentType: ${reqDataObj.contentType}`
      );

      bqInsertObj.gcsUrl = reqDataObj.gcsUrl;
      bqInsertObj.contentUrl =
        config.GCS_AUTH_BROWSER_URL_BASE +
        reqDataObj.gcsBucket +
        "/" +
        reqDataObj.gcsFile;
      bqInsertObj.contentType = reqDataObj.contentType;
      bqInsertObj.insertTimestamp = Math.round(Date.now() / 1000).toString();

      var features = [
        { type: "LOGO_DETECTION" },
        { type: "LABEL_DETECTION" },
        { type: "LANDMARK_DETECTION" },
        { type: "SAFE_SEARCH_DETECTION" },
      ];

      var request = {
        image: { source: { imageUri: reqDataObj.gcsUrl } },
        features,
      };

      console.info(`Vision request: ${JSON.stringify(request)}`);
      console.log(`Sending vision request`);
      return vision.annotateImage(request);
    })
    .then((results) => {
      const annotatedResponse = results[0];
      const logos = annotatedResponse.logoAnnotations;
      const labels = annotatedResponse.labelAnnotations;
      const safeSearch = annotatedResponse.safeSearchAnnotation;
      const error = annotatedResponse.error;
      console.info(`Received vision response ${JSON.stringify(results)}`);

      if (error !== null) {
        console.info("Error not null");
        console.error(
          `Error from Vision API: code:${error.code}, message: ${error.message} processing file ${reqDataObj.gcsUrl}`
        );
        throw new Error(
          `From Vision API: code:${error.code}, message: ${error.message} processing file ${reqDataObj.gcsUrl}`
        );
      }

      if (labels.length > 0) {
        bqInsertObj.labels = [];
        labels.forEach(function (label) {
          bqInsertObj = addALabel(label.description, bqInsertObj);
        });
      }

      bqInsertObj.safeSearch = [];
      bqInsertObj = addSafeSearchResults(
        config.API_Constants.ADULT,
        safeSearch.adult,
        bqInsertObj
      );
      bqInsertObj = addSafeSearchResults(
        config.API_Constants.SPOOF,
        safeSearch.spoof,
        bqInsertObj
      );
      bqInsertObj = addSafeSearchResults(
        config.API_Constants.MEDICAL,
        safeSearch.medical,
        bqInsertObj
      );
      bqInsertObj = addSafeSearchResults(
        config.API_Constants.VIOLENCE,
        safeSearch.violence,
        bqInsertObj
      );

      // check to see if any of the SafeSearch results came back with POSSIBLE, LIKELY, or VERY_LIKELY
      if (checkForSafeSearchLiklihood(safeSearch)) {
        // move the file and update the Uri and Url
        moveFile(
          reqDataObj.gcsBucket,
          reqDataObj.gcsFile,
          config.REJECTED_BUCKET,
          reqDataObj.gcsFile
        );
        bqInsertObj.gcsUrl =
          "gs://" + config.REJECTED_BUCKET + "/" + reqDataObj.gcsFile;
        bqInsertObj.contentUrl =
          config.GCS_AUTH_BROWSER_URL_BASE +
          config.REJECTED_BUCKET +
          "/" +
          reqDataObj.gcsFile;
      }

      if (logos.length > 0) {
        if (!bqInsertObj.labels) {
          bqInsertObj.labels = [];
        }

        logos.forEach(function (logo) {
          bqInsertObj = addALabel(logo.description, bqInsertObj);
        });
      }

      console.info(`bqInsertObj: ${JSON.stringify(bqInsertObj)}`);
      console.log("Publishing to bqinsert pubsub");
      return publishResult(config.BIGQUERY_TOPIC, bqInsertObj);
    })
    .then(() => {
      console.log(`File ${reqDataObj.gcsFile} processed.`);
    });
};
// [END functions_visionAPI]

// [START functions_videoIntelligenceAPI]
/**
 * Function to run a file through the Video Intelligence API and insert the results in BigQuery
 *
 * All SafeSearch annotations are aggregated and the highest rating for each 5 categories is returned.
 * i.e. if there are 3 sets of SafeSearch results and 1/3 indicates config.API_Constants.SPOOF is "VERT_LIKELY" while
 * the other 2 results indicate config.API_Constants.SPOOF as "UNKNOWN", config.API_Constants.SPOOF will be flagged as "VERY_LIKELY" for the video
 *
 * * @param {object} event The Cloud Functions event which contains a message with the GCS file details
 */
exports.videoIntelligenceAPI = function videoIntelligenceAPI(event) {
  // Imports the Google Cloud Video Intelligence library
  const videoIntelligence = require("@google-cloud/video-intelligence");

  // Creates a client
  const video = new videoIntelligence.VideoIntelligenceServiceClient();

  const reqData = Buffer.from(event.data, "base64").toString();
  const reqDataObj = JSON.parse(reqData);
  console.info(reqData);
  var bqInsertObj = {};

  return Promise.resolve()
    .then(() => {
      if (
        typeof reqDataObj.gcsBucket === "undefined" ||
        !reqDataObj.gcsBucket
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'Bucket not provided. Make sure you have a "gcsBucket" property in your request'
        );
      }
      if (typeof reqDataObj.gcsFile === "undefined" || !reqDataObj.gcsFile) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'Filename not provided. Make sure you have a "gcsFile" property in your request'
        );
      }
      if (
        typeof reqDataObj.contentType === "undefined" ||
        !reqDataObj.contentType
      ) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'ContentType not provided. Make sure you have a "contentType" property in your request'
        );
      }
      if (typeof reqDataObj.gcsUrl === "undefined" || !reqDataObj.gcsUrl) {
        console.error(`Input request: ${reqData}`);
        throw new Error(
          'GCS URL not provided. Make sure you have a "gcsUrl" property in your request'
        );
      }

      bqInsertObj.gcsUrl = reqDataObj.gcsUrl;
      bqInsertObj.contentUrl =
        config.GCS_AUTH_BROWSER_URL_BASE +
        reqDataObj.gcsBucket +
        "/" +
        reqDataObj.gcsFile;
      bqInsertObj.contentType = reqDataObj.contentType;
      bqInsertObj.insertTimestamp = Math.round(Date.now() / 1000).toString();

      console.log(
        `Received name: ${reqDataObj.gcsFile} and bucket: ${reqDataObj.gcsBucket} and contentType: ${reqDataObj.contentType}`
      );

      // Construct request
      const request = {
        inputUri: reqDataObj.gcsUrl,
        features: ["LABEL_DETECTION", "EXPLICIT_CONTENT_DETECTION"],
      };
      console.log(`Sending video intelligence request`);
      console.info(
        `Sending video intelligence request: ${JSON.stringify(request)}`
      );

      // Execute request
      return video.annotateVideo(request);
    })
    .then((results) => {
      const operation = results[0];
      console.log(
        "Waiting for operation to complete... (this may take a few minutes)"
      );
      return operation.promise();
    })
    .then((results) => {
      // Gets annotations for video
      const annotations = results[0].annotationResults[0];

      // update for https://cloud.google.com/video-intelligence/docs/release-notes
      console.log(`Received video intelligence response`);
      console.info(
        `Received video intelligence response: ${JSON.stringify(results)}`
      );

      const safeSearchAnnotations = annotations.explicitAnnotation;
      var safeSearchFlag = false;
      var safeSearchAggregator = [];
      safeSearchAggregator[config.API_Constants.ADULT] = 0;
      bqInsertObj.safeSearch = [];

      // get the explicitAnnotations from the Video Intelligence API results
      if (
        typeof safeSearchAnnotations === "undefined" ||
        !safeSearchAnnotations
      ) {
        console.error(
          `No explicitAnnotation included in response: ${JSON.stringify(
            safeSearchAnnotations
          )}`
        );
      } else if (
        typeof safeSearchAnnotations.frames === "undefined" ||
        !safeSearchAnnotations.frames
      ) {
        console.error(
          `No explicitAnnotation.frames included in response: ${JSON.stringify(
            safeSearchAnnotations
          )}`
        );
      } else if (safeSearchAnnotations.frames.length > 0) {
        safeSearchAnnotations.frames.forEach((safeSearchAnnotation) => {
          if (
            safeSearchAnnotation.pornographyLikelihood >
            safeSearchAggregator[config.API_Constants.ADULT]
          ) {
            safeSearchAggregator[config.API_Constants.ADULT] =
              safeSearchAnnotation.pornographyLikelihood;
          }

          if (!safeSearchFlag) {
            safeSearchFlag = checkVideoForSafeSearchLiklihood(
              safeSearchAnnotation
            );
          }
        });

        // check to see if any of the SafeSearch results were flagged and if so, move the file to a different GCS location
        if (safeSearchFlag) {
          // move the file and update the Uri and Url
          moveFile(
            reqDataObj.gcsBucket,
            reqDataObj.gcsFile,
            config.REJECTED_BUCKET,
            reqDataObj.gcsFile
          );
          bqInsertObj.gcsUrl =
            "gs://" + config.REJECTED_BUCKET + "/" + reqDataObj.gcsFile;
          bqInsertObj.contentUrl =
            config.GCS_AUTH_BROWSER_URL_BASE +
            config.REJECTED_BUCKET +
            "/" +
            reqDataObj.gcsFile;
        }
      }

      // add the explicitAnnotation results
      bqInsertObj = addSafeSearchResults(
        config.API_Constants.ADULT,
        videoSafeSearchMap[safeSearchAggregator[config.API_Constants.ADULT]],
        bqInsertObj
      );

      // Gets labels for video from its annotations
      const labels = annotations.segmentLabelAnnotations;
      if (
        typeof annotations.segmentLabelAnnotations === "undefined" ||
        !annotations.segmentLabelAnnotations
      ) {
        console.error(
          `No segmentLabelAnnotations results included in response: ${JSON.stringify(
            segmentLabelAnnotations
          )}`
        );
      } else if (labels.length > 0) {
        bqInsertObj.labels = [];
        labels.forEach((label) => {
          bqInsertObj = addALabel(label.entity.description, bqInsertObj);
        });
      }

      console.info(bqInsertObj);
      return publishResult(config.BIGQUERY_TOPIC, bqInsertObj);
    })
    .then(() => {
      console.log(`File ${reqDataObj.gcsFile} processed.`);
    });
};
// [END functions_videoIntelligenceAPI]

/**
 * Function to add a label to the request object
 *
 * @param {String} label The String label to add to the request
 * @param {object} requestObj The BigQuery request object
 */
function addALabel(label, requestObj) {
  var nameObj = {};
  nameObj.name = label;
  requestObj.labels.push(nameObj);
  return requestObj;
}

/**
 * Function to move a file from 1 GCS bucket to another
 *
 * @param {String} srcBucket The name of the source bucket
 * @param {String} srcFile The name of the source file
 * @param {String} destBucket The name of the destination bucket
 * @param {String} destFile The name of the destination file
 */
function moveFile(srcBucket, srcFile, destBucket, destFile) {
  const { Storage } = require("@google-cloud/storage");
  const newFileLoc = "gs://" + destBucket + "/" + destFile;
  const storage = new Storage();
  return storage
    .bucket(srcBucket)
    .file(srcFile)
    .move(newFileLoc)
    .then(() => {
      console.log(
        "gs://" +
          srcBucket +
          "/" +
          srcFile +
          " moved to gs://" +
          destBucket +
          "/" +
          destFile
      );
    });
}
/**
 * Function to add SafeSearchResults to the request object
 *
 * @param {String} safeSearchType The String name of the safeSearch result to add to the request
 * @param {object} bqInsertObj The current request object to which to add the safeSearch result
 */
function addSafeSearchResults(safeSearchType, safeSearchVal, bqInsertObj) {
  var flaggedTypeObj = {};
  flaggedTypeObj.flaggedType = safeSearchType;
  flaggedTypeObj.likelihood = safeSearchVal;
  bqInsertObj.safeSearch.push(flaggedTypeObj);
  return bqInsertObj;
}

/**
 * Checks whether any of the SafeSearch values are set and returns true if so, otherwise false
 *
 * @param {Object} safeSearch The SafeSearch object from the Vision API
 */
function checkForSafeSearchLiklihood(safeSearch) {
  if (checkSafeSearchLikelihood(safeSearch.adult)) return true;
  if (checkSafeSearchLikelihood(safeSearch.medical)) return true;
  if (checkSafeSearchLikelihood(safeSearch.violence)) return true;
  if (checkSafeSearchLikelihood(safeSearch.spoof)) return true;
}

/**
 * Checks whether the SafeSearch value is POSSIBLE, LIKELY, OR VERY_LIKELY and returns true if so, otherwise false
 *
 * @param {String} safeSearchResult The String value to be evaluated
 */
function checkSafeSearchLikelihood(safeSearchResult) {
  if (
    safeSearchResult == "POSSIBLE" ||
    safeSearchResult == "LIKELY" ||
    safeSearchResult == "VERY_LIKELY"
  ) {
    return true;
  } else {
    return false;
  }
}

/**
 * Checks whether any of the SafeSearch values are set and returns true if so, otherwise false
 *
 * @param {Object} safeSearch The SafeSearch object from the Video Intelligence API
 */
function checkVideoForSafeSearchLiklihood(safeSearch) {
  if (checkVideoSafeSearchLikelihood(safeSearch.pornographyLikelihood))
    return true;
  return false;
}

/**
 * Checks whether the SafeSearch value is POSSIBLE, LIKELY, OR VERY_LIKELY and returns true if so, otherwise false
 *
 * See the Video Intelligence proto on github for the details of the values
 * https://github.com/googleapis/googleapis/blob/master/google/cloud/videointelligence/v1beta1/video_intelligence.proto
 *
 * @param {int} safeSearchResult The int value to be evaluated
 */

function checkVideoSafeSearchLikelihood(safeSearchResult) {
  if (safeSearchResult == 3 || safeSearchResult == 4 || safeSearchResult == 5) {
    return true;
  } else {
    return false;
  }
}

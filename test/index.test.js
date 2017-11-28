/**
 * Copyright 2017, Google, Inc.
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

'use strict';

const proxyquire = require(`proxyquire`).noCallThru();
const sinon = require(`sinon`);
const test = require(`ava`);
const tools = require(`@google-cloud/nodejs-repo-tools`);
const Buffer = require('safe-buffer').Buffer;

const text = `text`;
const lang = `lang`;
const translation = `translation`;

const name = `file_name`;
const fileName = name;
const contentType = `text/plain`;
const bucketName = `intelligentcontentupload`;
const bucket = `intelligentcontentupload`;

const jpegName = "IMG_20170730_121827.jpg";
const jpegBucket = "intelligentcontent";
const jpegContentType = "image/jpeg";
const jpegGCSUrl = "gs://intelligentcontent/IMG_20170730_121827.jpg";
const jpegContentUrl = "https://storage.cloud.google.com/intelligentcontent/IMG_20170730_121827.jpg";
const pngContentType ="image/png";
const mp4Name ="test.mp4";
const mp4Bucket = "intelligentcontent";
const mp4ContentType = "video/mp4";
const textContentType = "text/plain";
const mp4GCSUrl = "gs://intelligentcontent/test.mp4";
const insertTimestamp = "1502811125";


const visionResponse1 = [
  { 
          faceAnnotations:[],
          landmarkAnnotations:[],
          logoAnnotations:[],
          labelAnnotations:[
            {
                mid:"/m/07s6nbt",locale:"",description:"document",score:0.9173216223716736,confidence:0,topicality:0,boundingPoly:null,locations:[],"properties":[]
            },
            {
                mid:"/m/03gq5hm",locale:"",description:"document",score:0.625476062297821,confidence:0,topicality:0,boundingPoly:null,locations:[],"properties":[]
            },
            {
                mid:"/m/015bv3",locale:"",description:"document",score:0.5243989825248718,confidence:0,topicality:0,boundingPoly:null,locations:[],"properties":[]
            }
          ],
          textAnnotations:[],
          fullTextAnnotation:"",
          safeSearchAnnotation:
          {
              adult:"VERY_UNLIKELY",spoof:"VERY_UNLIKELY",medical:"VERY_UNLIKELY",violence:"VERY_UNLIKELY"
          },
          imagePropertiesAnnotation:"",
          cropHintsAnnotation:"",
          webDetection:"",
          error: null
  }
];

const visionResponseError1 = [
  { 
          faceAnnotations:[],
          landmarkAnnotations:[],
          logoAnnotations:[],
          labelAnnotations:null,
          textAnnotations:[],
          fullTextAnnotation:null,
          safeSearchAnnotation:null,
          imagePropertiesAnnotation:null,
          cropHintsAnnotation:null,
          webDetection: null,
          error: {"code":3,"message":"Image size (5.29M) exceeding allowed max (4.00M).","details":[]}
  }
];


const videoIntelligenceAPIResponsev1beta2 = {
  annotationResults: [ 
  {
    inputUri: "/intelligentcontentfiltered/rox.mp4",
    segmentLabelAnnotations: [  
   {  
      entity:{  
         entityId:"/m/0bt9lr",
         description:"dog",
         languageCode:"en-US"
      },
      categoryEntities:[  
         {  
            entityId:"/m/068hy",
            description:"pet",
            languageCode:"en-US"
         }
      ],
      segments:[  
         {  
            segment:{  
               startTimeOffset:{  

               },
               endTimeOffset:{  
                  seconds:"2",
                  nanos:600960000
               }
            },
            confidence:0.9945738315582275
         }
      ]
   },
   {  
      entity:{  
         entityId:"/m/068hy",
         description:"pet",
         languageCode:"en-US"
      },
      categoryEntities:[  
         {  
            entityId:"/m/0jbk",
            description:"animal",
            languageCode:"en-US"
         }
      ],
      segments:[  
         {  
            segment:{  
               startTimeOffset:{  

               },
               endTimeOffset:{  
                  seconds:"2",
                  nanos:600960000
               }
            },
            confidence:0.9797834753990173
         }
      ]
     }
     
    ],
    explicitAnnotation: {
      frames: [ {
        timeOffset: {
          nanos: 792062000
        },
        pornographyLikelihood: 2
      }, {
        timeOffset: {
          seconds: 1,
          nanos: 771398000
        },
        pornographyLikelihood: 1
      } ]
    }
  } 
  ]
};

const bigQueryInsertRequest = [
{ 
  gcsUrl: 'gs://intelligentcontent/IMG_9432.jpg',
  contentUrl: 'https://storage.cloud.google.com/intelligentcontent/IMG_9432.jpg',
  contentType: 'image/jpeg',
  insertTimestamp: '1502811472',
  labels: 
   [ { name: 'red' },
     { name: 'light' },
     { name: 'macro photography' },
     { name: 'close up' },
     { name: 'automotive lighting' } ],
  safeSearch: 
   [ { flaggedType: 'adult', likelihood: 'VERY_UNLIKELY' },
     { flaggedType: 'spoof', likelihood: 'VERY_UNLIKELY' },
     { flaggedType: 'medical', likelihood: 'VERY_UNLIKELY' },
     { flaggedType: 'violence', likelihood: 'UNLIKELY' } ] 
}
];

function getSample (visionResponse,videoResponse) {
  const config = {
    RESULT_TOPIC: `result-topic`,
    RESULT_BUCKET: `result-bucket`,

    VISION_TOPIC: "projects/your-secret-project-id/topics/visionapiservice",
    VIDEOINTELLIGENCE_TOPIC: "projects/your-secret-project-id/topics/videointelligenceservice",
    BIGQUERY_TOPIC: "projects/your-secret-project-id/topics/bqinsert",
    REJECTED_BUCKET: "intelligentcontentfiltered",
    RESULT_BUCKET: "intelligentcontent",
    DATASET_ID: "intelligentcontentfilter",
    TABLE_NAME: "filtered_content",
    GCS_AUTH_BROWSER_URL_BASE: "https:\/\/storage.cloud.google.com\/" ,
    API_Constants: {
      ADULT : "adult",
      VIOLENCE : "violence",
      SPOOF : "spoof",
      MEDICAL : "medical"
    }
  };

  function mockLongRunningGrpcMethod(response,error) {
    var mockOperation = {
        promise: function() {
          return new Promise(function(resolve, reject) {
            if (error) {
              reject(error);
            } else {
              resolve([response]);
            }
          });
          return Promise.resolve([mockOperation]);
        }
    };

    return mockOperation;
  };

  const topic = {
    publish: sinon.stub().returns(Promise.resolve([]))
  };
  topic.get = sinon.stub().returns(Promise.resolve([topic]));
  const file = {
    save: sinon.stub().returns(Promise.resolve([])),
    move: sinon.stub().returns(Promise.resolve([])),
    bucket: bucketName,
    name: fileName
  };
  const bucket = {
    file: sinon.stub().returns(file)
  };
  const pubsubMock = {
    topic: sinon.stub().returns(topic)
  };
  const storageMock = {
    bucket: sinon.stub().returns(bucket)
  };
  const visionMock = {
    annotateImage: sinon.stub().returns(Promise.resolve(visionResponse))
  };
  const videoMock = {
    annotateVideo: sinon.stub().returns( Promise.resolve([mockLongRunningGrpcMethod(videoIntelligenceAPIResponsev1beta2)]))
  };
  const bigQueryTableMock = {
    insert: sinon.stub().returns(Promise.resolve([{ kind: 'bigquery#tableDataInsertAllResponse' }]))
  };
  const bigQueryDatasetMock = {
    table: sinon.stub().returns(bigQueryTableMock)
  };
  const bigqueryMock = {
    dataset: sinon.stub().returns(bigQueryDatasetMock)
  };
  const translateMock = {
    detect: sinon.stub().returns(Promise.resolve([{ language: `ja` }])),
    translate: sinon.stub().returns(Promise.resolve([translation]))
  };
  const PubsubMock = sinon.stub().returns(pubsubMock);
  const StorageMock = sinon.stub().returns(storageMock);
  const VisionMock = sinon.stub().returns(visionMock);
  const TranslateMock = sinon.stub().returns(translateMock);
  const VideoMock = sinon.stub().returns(videoMock);
  const BigQueryMock = sinon.stub().returns(bigqueryMock);


  return {
    program: proxyquire(`../`, {
      '@google-cloud/translate': TranslateMock,
      '@google-cloud/vision': VisionMock,
      '@google-cloud/video-intelligence': VideoMock,
      '@google-cloud/pubsub': PubsubMock,
      '@google-cloud/storage': StorageMock,
      '@google-cloud/bigquery': BigQueryMock,
      './config.json': config
    }),
    mocks: {
      config,
      pubsub: pubsubMock,
      storage: storageMock,
      bucket: bucket,
      file,
      vision: visionMock,
      video: videoMock,
      translate: translateMock,
      bigquery: bigqueryMock,
      topic
    }
  };
}



test.beforeEach(tools.stubConsole);
test.afterEach.always(tools.restoreConsole);

// 1. Tests for the GCStoPubsub method
test.serial(`GCStoPubsub fails without a bucket`, async (t) => {
  const error = new Error(`Bucket not provided. Make sure you have a "bucket" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({ name, contentType })).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.GCStoPubsub(event));
  t.deepEqual(err, error);
});

test.serial(`GCStoPubsub fails without a filename`, async (t) => {
  const error = new Error(`Filename not provided. Make sure you have a "name" property in your request`);
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({ contentType, bucket })).toString(`base64`)
    }
  };


  const err = await t.throws(getSample(null).program.GCStoPubsub(event));
  t.deepEqual(err, error);
});

test.serial(`GCStoPubsub fails without a contentType`, async (t) => {
  const error = new Error(`ContentType not provided. Make sure you have a "contentType" property in your request`);
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({ name, bucket })).toString(`base64`)
    }
  };


  const err = await t.throws(getSample(null).program.GCStoPubsub(event));
  t.deepEqual(err, error);
});

test.serial(`GCStoPubsub fails without a valid contentType`, async (t) => {
  const error = new Error(`Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request`);
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({ name, bucket, contentType })).toString(`base64`)
    }
  };


  const err = await t.throws(getSample(null).program.GCStoPubsub(event));
  t.deepEqual(err, error);
});

test.serial(`GCStoPubsub logs 4 messages when image contentType sent successfully`, async (t) => {
  
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({ name, bucket, contentType: jpegContentType })).toString(`base64`)
    }
  };

  await getSample(null).program.GCStoPubsub(event);
  
  t.deepEqual(console.log.getCall(0).args, [`Received name: ${name} and bucket: ${bucket} and contentType: image/jpeg`]);
  t.deepEqual(console.log.getCall(1).args, [`gs:\/\/${bucket}/${name} moved to gs:\/\/${getSample().mocks.config.RESULT_BUCKET}/${name}`]);
  t.deepEqual(console.log.getCall(2).args, [`Sending Vision API request`]);
  t.deepEqual(console.log.getCall(3).args, [`File ${name} processed.`]);
  t.is(console.log.callCount, 4);  

});

test.serial(`GCStoPubsub logs 4 messages when video contentType sent successfully`, async (t) => {
  
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({ name, bucket, contentType: mp4ContentType })).toString(`base64`)
    }
  };

  await getSample(null).program.GCStoPubsub(event);
  
  t.deepEqual(console.log.getCall(0).args, [`Received name: ${name} and bucket: ${bucket} and contentType: video/mp4`]);
  t.deepEqual(console.log.getCall(1).args, [`gs:\/\/${bucket}/${name} moved to gs:\/\/${getSample().mocks.config.RESULT_BUCKET}/${name}`]);
  t.deepEqual(console.log.getCall(2).args, [`Sending Video Intelligence API request`]);
  t.deepEqual(console.log.getCall(3).args, [`File ${name} processed.`]);
  t.is(console.log.callCount, 4);  

});

// 2. Tests for the visionAPI method
test.serial(`visionAPI fails without a gcsBucket`, async (t) => {
  const error = new Error(`Bucket not provided. Make sure you have a "gcsBucket" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: jpegContentType, gcsUrl:jpegGCSUrl,gcsFile: jpegName} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.visionAPI(event));
  t.deepEqual(err, error);
});

test.serial(`visionAPI fails without a gcsFile`, async (t) => {
  const error = new Error(`Filename not provided. Make sure you have a "gcsFile" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: jpegContentType, gcsUrl:jpegGCSUrl,gcsBucket: jpegBucket} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.visionAPI(event));
  t.deepEqual(err, error);
});

test.serial(`visionAPI fails without a contentType`, async (t) => {
  const error = new Error(`ContentType not provided. Make sure you have a "contentType" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl:jpegGCSUrl,gcsBucket: jpegBucket, gcsFile: jpegName} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.visionAPI(event));
  t.deepEqual(err, error);
});

test.serial(`visionAPI fails without a gcsUrl`, async (t) => {
  const error = new Error(`GCS URL not provided. Make sure you have a "gcsUrl" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: jpegContentType,gcsBucket: jpegBucket, gcsFile: jpegName} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.visionAPI(event));
  t.deepEqual(err, error);
});

test.serial(`visionAPI fails without a valid contentType`, async (t) => {
  const error = new Error(`Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request`);
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: textContentType, gcsUrl:jpegGCSUrl,gcsBucket: jpegBucket, gcsFile: jpegName})).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.visionAPI(event));
  t.deepEqual(err, error);
});


test.serial(`visionAPI logs 4 messages when video contentType sent successfully`, async (t) => {
  
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: jpegContentType, gcsUrl:jpegGCSUrl,gcsBucket: jpegBucket, gcsFile: jpegName} )).toString(`base64`)
    }
  };

  await getSample(visionResponse1).program.visionAPI(event);
  
  t.deepEqual(console.log.getCall(0).args, [`Received name: ${jpegName} and bucket: ${jpegBucket} and contentType: image/jpeg`]);
  t.deepEqual(console.log.getCall(1).args, [`Sending vision request`]);
  t.deepEqual(console.log.getCall(2).args, [`File ${jpegName} processed.`]);
  t.is(console.log.callCount, 3);
  
  });

test.serial(`visionAPI throws an error when API returns an error`, async (t) => {
  const visionAPIErrorCode = 3;
  const visionAPIErrorMessage = "Image size (5.29M) exceeding allowed max (4.00M).";

  const error = new Error(`From Vision API: code:${visionAPIErrorCode}, message: ${visionAPIErrorMessage} processing file gs:\/\/intelligentcontent\/IMG_20170730_121827.jpg`);
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: jpegContentType, gcsUrl:jpegGCSUrl, gcsBucket: jpegBucket, gcsFile: jpegName})).toString(`base64`)
    }
  };

  const err = await t.throws(getSample(visionResponseError1).program.visionAPI(event));
  t.deepEqual(err, error);
  t.deepEqual(console.error.getCall(0).args, [`Error from Vision API: code:3, message: Image size (5.29M) exceeding allowed max (4.00M). processing file gs:\/\/intelligentcontent\/IMG_20170730_121827.jpg`]);
  t.is(console.error.callCount, 1);
});


// 3. Tests for the videoIntelligenceAPI method
test.serial(`videoIntelligenceAPI fails without a gcsBucket`, async (t) => {
  const error = new Error(`Bucket not provided. Make sure you have a "gcsBucket" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: mp4ContentType, gcsUrl:jpegGCSUrl,gcsFile: jpegName} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.videoIntelligenceAPI(event));
  t.deepEqual(err, error);
});

test.serial(`videoIntelligenceAPI fails without a gcsFile`, async (t) => {
  const error = new Error(`Filename not provided. Make sure you have a "gcsFile" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: mp4ContentType, gcsUrl:jpegGCSUrl,gcsBucket: jpegBucket} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.videoIntelligenceAPI(event));
  t.deepEqual(err, error);
});

test.serial(`videoIntelligenceAPI fails without a contentType`, async (t) => {
  const error = new Error(`ContentType not provided. Make sure you have a "contentType" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl:jpegGCSUrl,gcsBucket: jpegBucket, gcsFile: jpegName} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.videoIntelligenceAPI(event));
  t.deepEqual(err, error);
});

test.serial(`videoIntelligenceAPI fails without a gcsUrl`, async (t) => {
  const error = new Error(`GCS URL not provided. Make sure you have a "gcsUrl" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: mp4ContentType,gcsBucket: jpegBucket, gcsFile: jpegName} )).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.videoIntelligenceAPI(event));
  t.deepEqual(err, error);
});



test.serial(`videoIntelligenceAPI logs 5 messages when video contentType sent successfully`, async (t) => {
  
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({contentType: mp4ContentType, gcsUrl:mp4GCSUrl, gcsBucket: mp4Bucket, gcsFile: mp4Name} )).toString(`base64`)
    }
  };

  await getSample(visionResponse1).program.videoIntelligenceAPI(event);
  
  
  t.deepEqual(console.log.getCall(0).args, [`Received name: ${mp4Name} and bucket: ${mp4Bucket} and contentType: ${mp4ContentType}`]);
  t.deepEqual(console.log.getCall(1).args, [`Sending video intelligence request`]);
  t.deepEqual(console.log.getCall(2).args, [`Waiting for operation to complete... (this may take a few minutes)`]);
  t.deepEqual(console.log.getCall(3).args, [`Received video intelligence response`]);
  t.deepEqual(console.log.getCall(4).args, [`File ${mp4Name} processed.`]);
  t.is(console.log.callCount, 5);
  

  });


// 4. Tests for the insertIntoBigQuery method
test.serial(`insertIntoBigQuery fails without a gcsUrl`, async (t) => {
  const error = new Error(`GCSUrl not provided. Make sure you have a "gcsUrl" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({})).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.insertIntoBigQuery(event));
  t.deepEqual(err, error);
});

test.serial(`insertIntoBigQuery fails without a contentUrl`, async (t) => {
  const error = new Error(`ContentUrl not provided. Make sure you have a "contentUrl" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl: jpegGCSUrl})).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.insertIntoBigQuery(event));
  t.deepEqual(err, error);
});

test.serial(`insertIntoBigQuery fails without a contentUrl`, async (t) => {
  const error = new Error(`ContentType not provided. Make sure you have a "contentType" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl: jpegGCSUrl, contentUrl: jpegContentUrl})).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.insertIntoBigQuery(event));
  t.deepEqual(err, error);
});


test.serial(`insertIntoBigQuery fails without a valid contentType`, async (t) => {
  const error = new Error(`Unsupported ContentType provided. Make sure you upload an image or video which includes a "contentType" property of image or video in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl: jpegGCSUrl, contentUrl: jpegContentUrl, contentType: textContentType})).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.insertIntoBigQuery(event));
  t.deepEqual(err, error);
});

test.serial(`insertIntoBigQuery fails without a valid insertTimestamp`, async (t) => {
  const error = new Error(`insertTimestamp not provided. Make sure you have a "insertTimestamp" property in your request`);

  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl: jpegGCSUrl, contentUrl: jpegContentUrl, contentType: jpegContentType})).toString(`base64`)
    }
  };
  const err = await t.throws(getSample(null).program.insertIntoBigQuery(event));
  t.deepEqual(err, error);
});


test.serial(`insertIntoBigQuery logs 3 calls when successful`, async (t) => {
  const event = {
    data: {
      data: Buffer.from(JSON.stringify({gcsUrl: jpegGCSUrl, contentUrl: jpegContentUrl, contentType: jpegContentType, insertTimestamp: insertTimestamp})).toString(`base64`)
    }
  };
  await getSample(null).program.insertIntoBigQuery(event);
  t.deepEqual(console.log.getCall(0).args, [`Sending BigQuery insert request`]);
  t.deepEqual(console.log.getCall(1).args, [`Inserted the record into BigQuery`]);
  t.deepEqual(console.log.getCall(2).args, [`Insert request complete`]);
  
});



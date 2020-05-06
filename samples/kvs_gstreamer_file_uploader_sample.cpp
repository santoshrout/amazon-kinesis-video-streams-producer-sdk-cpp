#include <gst/gst.h>
#include <gst/app/gstappsink.h>
#include <string.h>
#include <chrono>
#include <Logger.h>
#include "KinesisVideoProducer.h"
#include <vector>
#include <stdlib.h>
#include <mutex>
#include <IotCertCredentialProvider.h>

using namespace std;
using namespace com::amazonaws::kinesis::video;
using namespace log4cplus;

#ifdef __cplusplus
extern "C" {
#endif

int gstreamer_init(int, char **);

#ifdef __cplusplus
}
#endif

LOGGER_TAG("kvs_gstreamer_file_uploader");

#define DEFAULT_RETENTION_PERIOD_HOURS 2
#define DEFAULT_KMS_KEY_ID ""
#define DEFAULT_STREAMING_TYPE STREAMING_TYPE_OFFLINE
#define DEFAULT_AUDIO_VIDEO_CONTENT_TYPE "video/h264,audio/aac"
#define DEFAULT_VIDEO_ONLY_CONTENT_TYPE "video/h264"
#define DEFAULT_MAX_LATENCY_SECONDS 60
#define DEFAULT_FRAGMENT_DURATION_MILLISECONDS 2000
#define DEFAULT_TIMECODE_SCALE_MILLISECONDS 1
#define DEFAULT_KEY_FRAME_FRAGMENTATION TRUE
#define DEFAULT_FRAME_TIMECODES TRUE
#define DEFAULT_ABSOLUTE_FRAGMENT_TIMES TRUE
#define DEFAULT_FRAGMENT_ACKS TRUE
#define DEFAULT_RESTART_ON_ERROR TRUE
#define DEFAULT_RECALCULATE_METRICS TRUE
#define DEFAULT_STREAM_FRAMERATE 25
#define DEFAULT_AVG_BANDWIDTH_BPS (4 * 1024 * 1024)
#define DEFAULT_BUFFER_DURATION_SECONDS 120
#define DEFAULT_REPLAY_DURATION_SECONDS 40
#define DEFAULT_CONNECTION_STALENESS_SECONDS 60
#define DEFAULT_CODEC_ID "V_MPEG4/ISO/AVC"
#define DEFAULT_TRACKNAME "kinesis_video"
#define DEFAULT_FRAME_DURATION_MS 1
#define DEFAULT_AUDIO_VIDEO_DRIFT_TIMEOUT_SECOND 5

#define DEFAULT_VIDEO_TRACKID 1
#define DEFAULT_AUDIO_TRACK_NAME "audio"
#define DEFAULT_AUDIO_CODEC_ID "A_AAC"
#define DEFAULT_AUDIO_TRACKID 2

typedef struct _FileInfo {
    _FileInfo():
            path(""),
            last_fragment_ts(0) {}
    string path;
    uint64_t last_fragment_ts;
    mutex mtx;
} FileInfo;

typedef struct _CustomData {

    _CustomData():
            first_video_frame(true),
            eos_triggered(false),
            stream_status(STATUS_SUCCESS),
            key_frame_pts(0),
            main_loop(NULL) {
        producer_start_time = chrono::duration_cast<nanoseconds>(systemCurrentTime().time_since_epoch()).count();
    }

    GMainLoop *main_loop;
    GstElement *pipeline;

    unique_ptr<KinesisVideoProducer> kinesis_video_producer;
    shared_ptr<KinesisVideoStream> kinesis_video_stream;
    char *stream_name;

    mutex audio_video_sync_mtx;
    condition_variable audio_video_sync_cv;

    // file to upload
    FileInfo file;

    // indicate whether a video key frame has been received or not.
    volatile bool first_video_frame;

    // when uploading file, whether one of audio or video pipeline has reached eos.
    atomic_bool eos_triggered;

    // key:     trackId
    // value:   whether application has received the first frame for trackId.
    map<int, bool> stream_started;

    // stores any error status code reported by StreamErrorCallback.
    atomic_uint stream_status;

    // When uploading file, store the pts of frames that has flag FRAME_FLAG_KEY_FRAME. When the entire file has been uploaded,
    // key_frame_pts contains the timetamp of the last fragment in the file. key_frame_pts is then stored into last_fragment_ts
    // of the file.
    // Unit: ns
    uint64_t key_frame_pts;

    // Assuming frame timestamp are relative. Add producer_start_time to each frame's
    // timestamp to convert them to absolute timestamp. This way fragments dont overlap after token rotation when doing
    // file uploading.
    uint64_t producer_start_time;

    // The number of media types that we're uploading.
    //   1: video only mode
    //   2: audio video mode
    int total_track_count;

    unique_ptr<Credentials> credential;
} CustomData;

namespace com { namespace amazonaws { namespace kinesis { namespace video {

class SampleClientCallbackProvider : public ClientCallbackProvider {
public:

    UINT64 getCallbackCustomData() override {
        return reinterpret_cast<UINT64> (this);
    }

    StorageOverflowPressureFunc getStorageOverflowPressureCallback() override {
        return storageOverflowPressure;
    }

    static STATUS storageOverflowPressure(UINT64 custom_handle, UINT64 remaining_bytes);
};

class SampleStreamCallbackProvider : public StreamCallbackProvider {
    UINT64 custom_data_;
public:
    SampleStreamCallbackProvider(UINT64 custom_data) : custom_data_(custom_data) {}

    UINT64 getCallbackCustomData() override {
        return custom_data_;
    }

    StreamConnectionStaleFunc getStreamConnectionStaleCallback() override {
        return streamConnectionStaleHandler;
    };

    StreamErrorReportFunc getStreamErrorReportCallback() override {
        return streamErrorReportHandler;
    };

    DroppedFrameReportFunc getDroppedFrameReportCallback() override {
        return droppedFrameReportHandler;
    };

    FragmentAckReceivedFunc getFragmentAckReceivedCallback() override {
        return fragmentAckReceivedHandler;
    };

private:
    static STATUS
    streamConnectionStaleHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                 UINT64 last_buffering_ack);

    static STATUS
    streamErrorReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle, UPLOAD_HANDLE upload_handle, UINT64 errored_timecode,
                             STATUS status_code);

    static STATUS
    droppedFrameReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                              UINT64 dropped_frame_timecode);

    static STATUS
    fragmentAckReceivedHandler( UINT64 custom_data, STREAM_HANDLE stream_handle,
                                UPLOAD_HANDLE upload_handle, PFragmentAck pFragmentAck);
};

class SampleDeviceInfoProvider : public DefaultDeviceInfoProvider {
public:
    device_info_t getDeviceInfo() override {
        auto device_info = DefaultDeviceInfoProvider::getDeviceInfo();
        // Set the storage size to 128mb
        device_info.storageInfo.storageSize = 128 * 1024 * 1024;
        return device_info;
    }
};

STATUS
SampleClientCallbackProvider::storageOverflowPressure(UINT64 custom_handle, UINT64 remaining_bytes) {
    UNUSED_PARAM(custom_handle);
    LOG_WARN("Reporting storage overflow. Bytes remaining " << remaining_bytes);
    return STATUS_SUCCESS;
}

STATUS SampleStreamCallbackProvider::streamConnectionStaleHandler(UINT64 custom_data,
                                                                  STREAM_HANDLE stream_handle,
                                                                  UINT64 last_buffering_ack) {
    LOG_WARN("Reporting stream stale. Last ACK received " << last_buffering_ack);
    return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::streamErrorReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                                       UPLOAD_HANDLE upload_handle, UINT64 errored_timecode, STATUS status_code) {
    LOG_ERROR("Reporting stream error. Errored timecode: " << errored_timecode << " Status: "
                                                           << status_code);
    CustomData *data = reinterpret_cast<CustomData *>(custom_data);

    // Terminate pipeline if error is not retriable. We choose to terminate the pipeline on error because
    // the easiest way to recover is to stream the file from the beginning again.
    if (IS_RETRIABLE_ERROR(status_code) && data->main_loop != NULL) {
        LOG_WARN("Terminating pipeline due to unrecoverable stream error: " << status_code);
        data->stream_status = status_code;
        g_main_loop_quit(data->main_loop);
    }

    return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::droppedFrameReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                                        UINT64 dropped_frame_timecode) {
    LOG_WARN("Reporting dropped frame. Frame timecode " << dropped_frame_timecode);
    return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::fragmentAckReceivedHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                                         UPLOAD_HANDLE upload_handle, PFragmentAck pFragmentAck) {
    CustomData *data = reinterpret_cast<CustomData *>(custom_data);
    if (pFragmentAck->ackType == FRAGMENT_ACK_TYPE_PERSISTED) {
        std::unique_lock<std::mutex> lk(data->file.mtx);
        uint64_t last_frag_ts = data->file.last_fragment_ts /
                                duration_cast<nanoseconds>(milliseconds(DEFAULT_TIMECODE_SCALE_MILLISECONDS)).count();
        if (last_frag_ts != 0 && last_frag_ts == pFragmentAck->timestamp) {
            LOG_INFO("Successfully persisted file " << data->file.path);
        }
    }
    LOG_DEBUG("Reporting fragment ack received. Ack timecode " << pFragmentAck->timestamp);
    return STATUS_SUCCESS;
}

}  // namespace video
}  // namespace kinesis
}  // namespace amazonaws
}  // namespace com;

static void eos_cb(GstElement *sink, CustomData *data) {
    // If we only have video, we don't need to sync with the other track.
    if (data->total_track_count == 2 && !data->eos_triggered.load()) {
        // Media pipeline for one track has ended. Next time eos_cb is called means the entire file has been received.
        data->eos_triggered = true;
        data->audio_video_sync_cv.notify_all();
    } else {
        {
            std::unique_lock<std::mutex> lk(data->file.mtx);
            // store file's last fragment's timestamp.
            data->file.last_fragment_ts = data->key_frame_pts;
        }
        LOG_DEBUG("Terminating pipeline due to EOS");
        g_main_loop_quit(data->main_loop);
    }
}

bool put_frame(shared_ptr<KinesisVideoStream> kinesis_video_stream, void *data, size_t len, UINT64 track_id,
               const nanoseconds &pts, const nanoseconds &dts, FRAME_FLAGS flags) {
    Frame frame;
    frame.flags = flags;
    frame.decodingTs = static_cast<UINT64>(dts.count()) / DEFAULT_TIME_UNIT_IN_NANOS;
    frame.presentationTs = static_cast<UINT64>(pts.count()) / DEFAULT_TIME_UNIT_IN_NANOS;
    frame.duration = 0; // with audio, frame can get as close as 0.01ms
    frame.size = static_cast<UINT32>(len);
    frame.frameData = reinterpret_cast<PBYTE>(data);
    frame.trackId = track_id;
    return kinesis_video_stream->putFrame(frame);
}

bool all_stream_started(CustomData *data) {
    if (data->stream_started.size() < data->total_track_count) {
        return false;
    } else {
        for (map<int, bool>::iterator it = data->stream_started.begin(); it != data->stream_started.end(); ++it) {
            if (!it->second) {
                return false;
            }
        }
    }

    return true;
}

static GstFlowReturn on_new_sample(GstElement *sink, CustomData *data, int track_id) {
    std::unique_lock<std::mutex> lk(data->audio_video_sync_mtx);
    GstSample *sample = nullptr;
    GstBuffer *buffer;
    size_t buffer_size;
    bool delta, dropFrame;
    FRAME_FLAGS kinesis_video_flags;
    uint8_t *data_buffer;
    GstFlowReturn ret = GST_FLOW_OK;
    GstMapInfo info;

    info.data = nullptr;
    sample = gst_app_sink_pull_sample(GST_APP_SINK (sink));

    // extract cpd for the first frame for each track
    if (!data->stream_started[track_id]) {
        data->stream_started[track_id] = true;
        GstCaps *gstcaps = (GstCaps *) gst_sample_get_caps(sample);
        GST_LOG("caps are %" GST_PTR_FORMAT, gstcaps);
        GstStructure *gststructforcaps = gst_caps_get_structure(gstcaps, 0);
        const GValue *gstStreamFormat = gst_structure_get_value(gststructforcaps, "codec_data");
        gchar *cpd = gst_value_serialize(gstStreamFormat);
        data->kinesis_video_stream->start(std::string(cpd), track_id);
        g_free(cpd);

        // dont block waiting for cpd if pipeline state is not GST_STATE_PLAYING, otherwise it will
        // block pipeline state transition.
        if (GST_STATE(data->pipeline) != GST_STATE_PLAYING) {
            goto CleanUp;
        }

        // block pipeline until cpd for all tracks have been received. Otherwise we will get STATUS_INVALID_STREAM_STATE
        if (!all_stream_started(data)) {
            data->audio_video_sync_cv.wait_for(lk, seconds(DEFAULT_AUDIO_VIDEO_DRIFT_TIMEOUT_SECOND), [data]{
                return all_stream_started(data);
            });

            if(!all_stream_started(data)) {
                LOG_ERROR("Drift between audio and video is above threshold");
                ret = GST_FLOW_ERROR;
                goto CleanUp;
            }
        } else {
            data->audio_video_sync_cv.notify_all();
        }
    }

    buffer = gst_sample_get_buffer(sample);
    buffer_size = gst_buffer_get_size(buffer);

    dropFrame =  GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_CORRUPTED) ||
                 GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DECODE_ONLY) ||
                 (GST_BUFFER_FLAGS(buffer) == GST_BUFFER_FLAG_DISCONT) ||
                 (GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DISCONT) && GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT)) ||
                 (!GST_BUFFER_PTS_IS_VALID(buffer)); //frame with invalid pts cannot be processed.

    if (!dropFrame) {
        delta = GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT);

        if (!delta && track_id == DEFAULT_VIDEO_TRACKID) {
            if (data->first_video_frame) {
                // start cutting fragment at second video key frame because we can have audio frames before first video key frame
                data->first_video_frame = false;
            } else {
                kinesis_video_flags = FRAME_FLAG_KEY_FRAME;
            }
        }

        // Always synthesize dts for file sources because file sources dont have meaningful dts.
        buffer->dts += DEFAULT_FRAME_DURATION_MS * HUNDREDS_OF_NANOS_IN_A_MILLISECOND * DEFAULT_TIME_UNIT_IN_NANOS;

        // convert relative time to absolute
        buffer->pts += data->producer_start_time;

        if (CHECK_FRAME_FLAG_KEY_FRAME(kinesis_video_flags)) {
            data->key_frame_pts = buffer->pts;
        }

        if (!gst_buffer_map(buffer, &info, GST_MAP_READ)){
            goto CleanUp;
        }

        put_frame(data->kinesis_video_stream, info.data, info.size, track_id, std::chrono::nanoseconds(buffer->pts),
                               std::chrono::nanoseconds(buffer->dts), kinesis_video_flags);
    }

CleanUp:

    if (info.data != nullptr) {
        gst_buffer_unmap(buffer, &info);
    }

    if (sample != nullptr) {
        gst_sample_unref(sample);
    }

    return ret;
}

GstFlowReturn on_new_sample_video(GstElement *sink, CustomData* data) {
    return on_new_sample(sink, data, DEFAULT_VIDEO_TRACK_ID);
}

GstFlowReturn on_new_sample_audio(GstElement *sink, CustomData* data) {
    return on_new_sample(sink, data, DEFAULT_AUDIO_TRACK_ID);
}

/* This function is called when an error message is posted on the bus */
static void error_cb(GstBus *bus, GstMessage *msg, CustomData *data) {
    GError *err;
    gchar *debug_info;

    /* Print error details on the screen */
    gst_message_parse_error(msg, &err, &debug_info);
    g_printerr("Error received from element %s: %s\n", GST_OBJECT_NAME (msg->src), err->message);
    g_printerr("Debugging information: %s\n", debug_info ? debug_info : "none");
    g_clear_error(&err);
    g_free(debug_info);

    g_main_loop_quit(data->main_loop);
}

void kinesis_video_init(CustomData *data) {
    unique_ptr<DeviceInfoProvider> device_info_provider(new SampleDeviceInfoProvider());
    unique_ptr<ClientCallbackProvider> client_callback_provider(new SampleClientCallbackProvider());
    unique_ptr<StreamCallbackProvider> stream_callback_provider(new SampleStreamCallbackProvider(
            reinterpret_cast<UINT64>(data)));

    char const *accessKey;
    char const *secretKey;
    char const *sessionToken;
    char const *defaultRegion;
    string defaultRegionStr;
    string sessionTokenStr;

    char const *iot_get_credential_endpoint;
    char const *cert_path;
    char const *private_key_path;
    char const *role_alias;
    char const *ca_cert_path;

    unique_ptr<CredentialProvider> credential_provider;

    if (nullptr == (defaultRegion = getenv(DEFAULT_REGION_ENV_VAR))) {
        defaultRegionStr = DEFAULT_AWS_REGION;
    } else {
        defaultRegionStr = string(defaultRegion);
    }
    LOG_INFO("Using region: " << defaultRegionStr);

    if (nullptr != (accessKey = getenv(ACCESS_KEY_ENV_VAR)) &&
        nullptr != (secretKey = getenv(SECRET_KEY_ENV_VAR))) {

        LOG_INFO("Using aws credentials for Kinesis Video Streams");
        if (nullptr != (sessionToken = getenv(SESSION_TOKEN_ENV_VAR))) {
            LOG_INFO("Session token detected.");
            sessionTokenStr = string(sessionToken);
        } else {
            LOG_INFO("No session token was detected.");
            sessionTokenStr = "";
        }

        data->credential.reset(new Credentials(string(accessKey),
                                               string(secretKey),
                                               sessionTokenStr));
        credential_provider.reset(new StaticCredentialProvider(*data->credential.get()));

    } else if (nullptr != (iot_get_credential_endpoint = getenv("IOT_GET_CREDENTIAL_ENDPOINT")) &&
               nullptr != (cert_path = getenv("CERT_PATH")) &&
               nullptr != (private_key_path = getenv("PRIVATE_KEY_PATH")) &&
               nullptr != (role_alias = getenv("ROLE_ALIAS")) &&
               nullptr != (ca_cert_path = getenv("CA_CERT_PATH"))) {
        LOG_INFO("Using IoT credentials for Kinesis Video Streams");
        credential_provider.reset(new IotCertCredentialProvider(iot_get_credential_endpoint,
                                                                cert_path,
                                                                private_key_path,
                                                                role_alias,
                                                                ca_cert_path,
                                                                data->stream_name));

    } else {
        LOG_AND_THROW("No valid credential method was found");
    }

    data->kinesis_video_producer = KinesisVideoProducer::createSync(move(device_info_provider),
                                                                    move(client_callback_provider),
                                                                    move(stream_callback_provider),
                                                                    move(credential_provider),
                                                                    defaultRegionStr);

    LOG_DEBUG("Client is ready");
}

void kinesis_video_stream_init(CustomData *data) {
    unique_ptr<StreamDefinition> stream_definition(new StreamDefinition(
        data->stream_name,
        hours(DEFAULT_RETENTION_PERIOD_HOURS),
        nullptr,
        DEFAULT_KMS_KEY_ID,
        DEFAULT_STREAMING_TYPE,
        data->total_track_count == 1 ? DEFAULT_VIDEO_ONLY_CONTENT_TYPE : DEFAULT_AUDIO_VIDEO_CONTENT_TYPE,
        duration_cast<milliseconds> (seconds(DEFAULT_MAX_LATENCY_SECONDS)),
        milliseconds(DEFAULT_FRAGMENT_DURATION_MILLISECONDS),
        milliseconds(DEFAULT_TIMECODE_SCALE_MILLISECONDS),
        DEFAULT_KEY_FRAME_FRAGMENTATION,
        DEFAULT_FRAME_TIMECODES,
        DEFAULT_ABSOLUTE_FRAGMENT_TIMES,
        DEFAULT_FRAGMENT_ACKS,
        DEFAULT_RESTART_ON_ERROR,
        DEFAULT_RECALCULATE_METRICS,
        NAL_ADAPTATION_FLAG_NONE,
        DEFAULT_STREAM_FRAMERATE,
        DEFAULT_AVG_BANDWIDTH_BPS,
        seconds(DEFAULT_BUFFER_DURATION_SECONDS),
        seconds(DEFAULT_REPLAY_DURATION_SECONDS),
        seconds(DEFAULT_CONNECTION_STALENESS_SECONDS),
        DEFAULT_CODEC_ID,
        DEFAULT_TRACKNAME,
        nullptr,
        0));

    if (data->total_track_count == 2) {
        stream_definition->addTrack(DEFAULT_AUDIO_TRACK_ID, DEFAULT_AUDIO_TRACK_NAME, DEFAULT_AUDIO_CODEC_ID, MKV_TRACK_INFO_TYPE_AUDIO);
    }

    data->kinesis_video_stream = data->kinesis_video_producer->createStreamSync(move(stream_definition));

    // reset state
    data->stream_status = STATUS_SUCCESS;
    data->stream_started.clear();

    LOG_DEBUG("Stream is ready");
}

int gstreamer_init(int argc, char* argv[], CustomData *data) {
    GstElement *pipeline, *appsink_video, *appsink_audio;
    GstStateChangeReturn gst_ret;
    GError *error = NULL;
    string file_path = data->file.path;
    const char* demuxer = NULL;
    char pipeline_buf[4096];
    int ret;

    // reset state
    data->eos_triggered = false;

    /* init GStreamer */
    gst_init(&argc, &argv);

    LOG_INFO("Streaming from file source");

    string file_suffix = file_path.substr(file_path.size() - 3);
    if (file_suffix.compare("mkv") == 0) {
        demuxer = "matroskademux";
    } else if (file_suffix.compare("mp4") == 0) {
        demuxer = "qtdemux";
    } else if (file_suffix.compare(".ts") == 0) {
        demuxer = "tsdemux";
    } else {
        LOG_ERROR("File format not supported. Supported ones are mp4, mkv and ts. File suffix: " << file_suffix);
        return 1;
    }

    if (data->total_track_count == 1) { // video only
        ret = snprintf(pipeline_buf, sizeof(pipeline_buf),
            "filesrc location=%s ! %s name=demuxer ! "
            "h264parse ! video/x-h264,stream-format=avc,alignment=au ! appsink sync=TRUE emit-signals=TRUE name=appsink-video",
            file_path.c_str(), demuxer
        );
    } else { // audio-video
        ret = snprintf(pipeline_buf, sizeof(pipeline_buf),
            "filesrc location=%s ! %s name=demuxer "
            "demuxer. ! queue ! h264parse ! video/x-h264,stream-format=avc,alignment=au ! appsink sync=TRUE emit-signals=TRUE name=appsink-video "
            "demuxer. ! queue ! aacparse ! audio/mpeg,stream-format=raw ! appsink sync=TRUE emit-signals=TRUE name=appsink-audio",
            file_path.c_str(), demuxer
        );
    }
    if (ret < 0) {
        LOG_ERROR("File path is too long. File path: " << file_path);
        return ret;
    }

    pipeline = gst_parse_launch(pipeline_buf, &error);
    appsink_video = gst_bin_get_by_name(GST_BIN(pipeline), "appsink-video");
    appsink_audio = gst_bin_get_by_name(GST_BIN(pipeline), "appsink-audio");

    if (!appsink_video) {
        LOG_ERROR("can't find appsink-video");
        return 1;
    }

    if (!appsink_audio && data->total_track_count == 2) {
        LOG_ERROR("can't find appsink-audio");
        return 1;
    }

    data->pipeline = pipeline;

    /* Set new-sample and eos callbacks */
    if (appsink_video) {
        g_signal_connect(appsink_video, "new-sample", G_CALLBACK(on_new_sample_video), data);
        g_signal_connect(appsink_video, "eos", G_CALLBACK(eos_cb), data);
    }

    if (appsink_audio) {
        g_signal_connect(appsink_audio, "new-sample", G_CALLBACK(on_new_sample_audio), data);
        g_signal_connect(appsink_audio, "eos", G_CALLBACK(eos_cb), data);
    }

    /* Instruct the bus to emit signals for each received message, and connect to the interesting signals */
    GstBus *bus = gst_element_get_bus(data->pipeline);
    gst_bus_add_signal_watch(bus);
    g_signal_connect (G_OBJECT(bus), "message::error", (GCallback) error_cb, data);
    gst_object_unref(bus);

    /* start streaming */
    gst_ret = gst_element_set_state(data->pipeline, GST_STATE_PLAYING);
    if (gst_ret == GST_STATE_CHANGE_FAILURE) {
        g_printerr("Unable to set the pipeline to the playing state.\n");
        gst_object_unref(data->pipeline);
        return 1;
    }

    data->main_loop = g_main_loop_new(NULL, FALSE);
    g_main_loop_run(data->main_loop);

    /* free resources */
    gst_bus_remove_signal_watch(bus);
    gst_element_set_state(data->pipeline, GST_STATE_NULL);
    gst_object_unref(data->pipeline);
    g_main_loop_unref(data->main_loop);
    data->main_loop = NULL;
    return 0;
}

int main(int argc, char* argv[]) {
    PropertyConfigurator::doConfigure("../samples/kvs_log_configuration");

    if (argc < 3 || argc > 4) {
        LOG_ERROR(
                "Usage: AWS_ACCESS_KEY_ID=SAMPLEKEY AWS_SECRET_ACCESS_KEY=SAMPLESECRET "
                "./kinesis_video_gstreamer_sample_app <stream-name> <path/to/file.mp4> [video-only|audio-video]\n");
        return 1;
    }

    const int PUTFRAME_FAILURE_RETRY_COUNT = 3;

    CustomData data;
    char stream_name[MAX_STREAM_NAME_LEN + 1];
    int ret = 0;
    int file_retry_count = PUTFRAME_FAILURE_RETRY_COUNT;
    STATUS stream_status = STATUS_SUCCESS;

    STRNCPY(stream_name, argv[1], MAX_STREAM_NAME_LEN);
    stream_name[MAX_STREAM_NAME_LEN] = '\0';
    data.stream_name = stream_name;

    string file_path = string(argv[2]);
    data.file.path = file_path;
    data.total_track_count = 1;

    if (argc > 3) {
        if (strcmp(argv[3], "audio-video") == 0) {
            LOG_DEBUG("Uploading audio and video");
            data.total_track_count = 2;
        } else if (strcmp(argv[3], "video-only") == 0) {
            LOG_DEBUG("Uploading video only");
        } else {
            LOG_DEBUG("Unrecognized upload type. Default to video-only");
        }
    } else {
        LOG_DEBUG("No upload type specified. Default to video-only");
    }

    /* init Kinesis Video */
    try{
        kinesis_video_init(&data);
        kinesis_video_stream_init(&data);
    } catch (runtime_error &err) {
        LOG_ERROR("Failed to initialize kinesis video with an exception: " << err.what());
        return 1;
    }

    bool do_retry = true;

    do {
        LOG_DEBUG("Attempt to upload file: " << data.file.path);

        // control will return after gstreamer_init after file eos or any GST_ERROR was put on the bus.
        gstreamer_init(argc, argv, &data);

        // check if any stream error occurred.
        stream_status = data.stream_status.load();

        if (STATUS_FAILED(stream_status)) {
            data.kinesis_video_stream->stop();
            if (!IS_RETRIABLE_ERROR(stream_status)) {
                LOG_ERROR("Fatal stream error occurred: " << stream_status << ". Terminating.");
                do_retry = false;
            }
        } else {
            LOG_INFO("Finished sending file to kvs producer: " << data.file.path);
            // stop sync will send out remaining frames. If stopSync
            // succeeds then everything is done, otherwise do retry
            if (data.kinesis_video_stream->stopSync()) {
                LOG_INFO(data.file.path << " has been persisted");
                do_retry = false;
            }
        }

        if (do_retry) {
            file_retry_count--;
            if (file_retry_count == 0) {
                LOG_ERROR("Failed to upload file " << data.file.path << " after retrying. Terminating.");
                do_retry = false;
            }
        }
    } while(do_retry);

    // CleanUp
    data.kinesis_video_producer->freeStream(data.kinesis_video_stream);

    return 0;
}

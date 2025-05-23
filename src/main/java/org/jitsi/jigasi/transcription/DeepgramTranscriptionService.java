package org.jitsi.jigasi.transcription;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.client.*;
import org.json.simple.*;
import org.json.simple.parser.*;
import org.jitsi.jigasi.*;
import org.jitsi.utils.logging.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public class DeepgramTranscriptionService extends AbstractTranscriptionService
{

    private final static Logger logger = Logger.getLogger(DeepgramTranscriptionService.class);

    public final static String WEBSOCKET_URL =
            "org.jitsi.jigasi.transcription.deepgram.websocket_url";

    public final static String API_KEY =
            "org.jitsi.jigasi.transcription.deepgram.api_key";

    public final static String DEFAULT_WEBSOCKET_URL =
            "wss://api.deepgram.com/v1/listen";

    private final static String EOF_MESSAGE = "{ \"type\": \"CloseStream\" }";

    private final String apiKey;

    private final String websocketUrlConfig;

    private final JSONParser jsonParser = new JSONParser();

    private String websocketUrl;

    private void generateWebsocketUrl(Participant participant)
    {
        websocketUrl = websocketUrlConfig + "?language="
                + participant.getSourceLanguage()
                + "&interim_results=true"
                + "&encoding=linear16"
                + "&sample_rate=48000";
    }

    public DeepgramTranscriptionService() {
        websocketUrlConfig = JigasiBundleActivator.getConfigurationService()
                .getString(WEBSOCKET_URL, DEFAULT_WEBSOCKET_URL);
        apiKey = JigasiBundleActivator.getConfigurationService()
                .getString(API_KEY);
    }

    @Override
    public boolean supportsFragmentTranscription()
    {
        return true;
    }

    @Override
    public void sendSingleRequest(TranscriptionRequest request, Consumer<TranscriptionResult> resultConsumer)
            throws UnsupportedOperationException
    {
        try
        {
            WebSocketClient ws = new WebSocketClient();
            DeepgramWebsocketSession socket = new DeepgramWebsocketSession(request);
            ws.start();
            ClientUpgradeRequest clientUpgradeRequest = new ClientUpgradeRequest();
            clientUpgradeRequest.setHeader("Authorization", "Token " + apiKey);
            ws.connect(socket, new URI(websocketUrl), clientUpgradeRequest);
            socket.awaitClose();
            resultConsumer.accept(
                    new TranscriptionResult(
                      null,
                      socket.getUuid(),
                      Instant.now(),
                      !socket.isFinal(),
                      request.getLocale().toLanguageTag(),
                      0,
                      new TranscriptionAlternative(socket.getResult())
                    ));
        }
        catch (Exception e)
        {
            logger.error("Error sending single req", e);
        }
    }

    @Override
    public boolean supportsStreamRecognition()
    {
        return true;
    }

    @Override
    public boolean supportsLanguageRouting()
    {
        return true;
    }

    @Override
    public StreamingRecognitionSession initStreamingSession(Participant participant)
            throws UnsupportedOperationException
    {
        try
        {
            generateWebsocketUrl(participant);
            DeepgramWebsocketStreamingSession streamingSession = new DeepgramWebsocketStreamingSession(
                    participant.getDebugName(), this.apiKey);
            streamingSession.transcriptionTag = participant.getTranslationLanguage();
            if (streamingSession.transcriptionTag == null)
            {
                streamingSession.transcriptionTag = participant.getSourceLanguage();
            }
            return streamingSession;
        }
        catch (Exception e)
        {
            throw new UnsupportedOperationException("Failed to create streaming session", e);
        }
    }

    @Override
    public boolean isConfiguredProperly()
    {
        return JigasiBundleActivator.getConfigurationService()
                .getString(API_KEY) != null;
    }

    @WebSocket
    public class DeepgramWebsocketStreamingSession
        implements StreamingRecognitionSession
    {
        private Session session;

        private final String debugName;

        private String transcriptionTag = "en-US";

        private String lastResult = "";

        private UUID uuid = UUID.randomUUID();

        private final List<TranscriptionListener> listeners = new ArrayList<>();

        DeepgramWebsocketStreamingSession(String debugName, String apiKey)
                throws Exception
        {
            this.debugName = debugName;
            WebSocketClient ws = new WebSocketClient();
            ws.start();
            ClientUpgradeRequest clientUpgradeRequest = new ClientUpgradeRequest();
            clientUpgradeRequest.setHeader("Authorization", "Token " + apiKey);
            ws.connect(this, new URI(websocketUrl), clientUpgradeRequest);
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason)
        {
            this.session = null;
        }

        @OnWebSocketConnect
        public void onConnect(Session session)
        {
            this.session = session;
        }

        @OnWebSocketMessage
        public void onMessage(String msg)
        {
            try
            {
                this.onMessageInternal(msg);
            }
            catch (ParseException e)
            {
                logger.error("Error parsing message", e);
            }
        }

        private void onMessageInternal(String msg)
            throws ParseException
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(debugName + " Received response: " + msg);
            }
            JSONObject obj = (JSONObject) jsonParser.parse(msg);
            // Retrieve "is_final" as a boolean
            boolean isFinal = obj.containsKey("is_final") && Boolean.TRUE.equals(obj.get("is_final"));

            // Extract "transcript" if it exists within nested objects
            String result = "";
            if (obj.containsKey("channel"))
            {
                JSONObject channel = (JSONObject) obj.get("channel");
                if (channel.containsKey("alternatives"))
                {
                    JSONArray alternatives = (JSONArray) channel.get("alternatives");
                    if (!alternatives.isEmpty())
                    {
                        JSONObject firstAlternative = (JSONObject) alternatives.get(0);
                        result = (String) firstAlternative.get("transcript");
                    }
                }
            }

            // Extract "confidence" as a double
            double confidence = 0.0;
            if (obj.containsKey("channel"))
            {
                JSONObject channel = (JSONObject) obj.get("channel");
                if (channel.containsKey("alternatives"))
                {
                    JSONArray alternatives = (JSONArray) channel.get("alternatives");
                    if (!alternatives.isEmpty())
                    {
                        JSONObject firstAlternative = (JSONObject) alternatives.get(0);
                        Object confidenceValue = firstAlternative.get("confidence");
                        confidence = confidenceValue != null ? ((Number) confidenceValue).doubleValue() : 0.0;
                    }
                }
            }
            if (logger.isDebugEnabled())
            {
                logger.debug(debugName + " parsed result " + result);
            }
            if (!result.isEmpty() && (isFinal || !result.equals(lastResult)))
            {
                lastResult = result;
                for (TranscriptionListener l : listeners)
                {
                    l.notify(new TranscriptionResult(
                            null,
                            uuid,
                            Instant.now(),
                            !isFinal,
                            transcriptionTag,
                            0.0,
                            new TranscriptionAlternative(result, confidence)));
                }
            }

            if (isFinal)
            {
                this.uuid = UUID.randomUUID();
            }
        }

        @OnWebSocketError
        public void onError(Throwable cause)
        {
            logger.error("Error while streaming audio data to transcription service", cause);
        }

        @Override
        public void sendRequest(TranscriptionRequest request)
        {
            try
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("sendRequest bytes: " + request.getAudio().length);
                }
                ByteBuffer audioBuffer = ByteBuffer.wrap(request.getAudio());
                session.getRemote().sendBytes(audioBuffer);
            }
            catch (Exception e)
            {
                logger.error("Error to send websocket request for participant " + debugName, e);
            }
        }

        @Override
        public void end()
        {
            try
            {
                session.getRemote().sendString(EOF_MESSAGE);
            }
            catch (Exception e)
            {
                logger.error("Error to finalize websocket connection for participant " + debugName, e);
            }
        }

        @Override
        public boolean ended()
        {
            return session == null;
        }

        @Override
        public void addTranscriptionListener(TranscriptionListener listener)
        {
            listeners.add(listener);
        }
    }

    @WebSocket
    public class DeepgramWebsocketSession
    {

        private final CountDownLatch closeLatch;

        private final TranscriptionRequest request;

        private String result = "";

        private UUID uuid;

        private boolean isFinal;

        DeepgramWebsocketSession(TranscriptionRequest request)
        {
            this.closeLatch = new CountDownLatch(1);
            this.request = request;
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason)
        {
            this.closeLatch.countDown();
        }

        @OnWebSocketConnect
        public void onConnect(Session session)
        {
            try
            {
                ByteBuffer audioBuffer = ByteBuffer.wrap(request.getAudio());
                session.getRemote().sendBytes(audioBuffer);
                session.getRemote().sendString(EOF_MESSAGE);
            }
            catch (IOException e)
            {
                logger.error("Error to transcribe audio", e);
            }
        }

        private String getTranscript(JSONArray jsonArray)
        {
            JSONObject obj = (JSONObject) jsonArray.get(0);
            return (String) obj.get("transcript");
        }

        @OnWebSocketMessage
        public void onMessage(String msg)
        {
            try
            {
                this.onMessageInternal(msg);
            }
            catch (ParseException e)
            {
                logger.error("Error parsing message: " + msg, e);
            }
        }

        private void onMessageInternal(String msg)
            throws ParseException
        {
            JSONObject obj = (JSONObject) jsonParser.parse(msg);
            this.isFinal = obj.containsKey("is_final") && Boolean.TRUE.equals(obj.get("is_final"));
            this.result = "";
            if (obj.containsKey("channel"))
            {
                JSONObject channel = (JSONObject) obj.get("channel");
                if (channel.containsKey("alternatives"))
                {
                    JSONArray alternatives = (JSONArray) channel.get("alternatives");
                    if (!alternatives.isEmpty())
                    {
                        this.result = getTranscript(alternatives);
                    }
                }
            }
            if  (obj.containsKey("metadata"))
            {
                JSONObject metadata = (JSONObject) obj.get("metadata");
                if (metadata.containsKey("request_id"))
                {
                    this.uuid = UUID.fromString((String) metadata.get("request_id"));
                }
                else if (obj.containsKey("request_id"))
                {
                    this.uuid = UUID.fromString((String) obj.get("request_id"));
                }
            }
        }

        @OnWebSocketError
        public void onError(Throwable cause)
        {
            logger.error("Websocket connection error", cause);
        }

        public String getResult()
        {
            return result;
        }

        void awaitClose()
                throws InterruptedException
        {
            closeLatch.await();
        }


        public UUID getUuid()
        {
            return uuid;
        }

        public boolean isFinal()
        {
            return isFinal;
        }
    }
}

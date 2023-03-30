package org.megras.segmentation

import com.github.kokorin.jaffree.StreamType
import com.github.kokorin.jaffree.ffmpeg.*
import com.github.kokorin.jaffree.ffprobe.FFprobe
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.megras.api.rest.RestErrorStatus
import java.awt.image.BufferedImage
import java.nio.channels.SeekableByteChannel
import java.util.concurrent.TimeUnit


object VideoSegmenter {

    fun segment(videoStream: SeekableByteChannel, segmentation: Segmentation): SeekableInMemoryByteChannel? = try {
        when(segmentation.type) {
            SegmentationType.RECT -> segmentRect(videoStream, segmentation as Rect)
            SegmentationType.POLYGON,
            SegmentationType.SPLINE,
            SegmentationType.PATH,
            SegmentationType.MASK -> segmentShape(videoStream, segmentation)
            SegmentationType.TIME -> segmentTime(videoStream, segmentation as Time)
            SegmentationType.CHANNEL -> segmentChannel(videoStream, segmentation as Channel)
            else -> null
        }
    } catch (e: Exception) {
        //TODO log
        null
    }

    private fun segmentRect(videoStream: SeekableByteChannel, rect: Rect): SeekableInMemoryByteChannel {
        val out = SeekableInMemoryByteChannel()
        FFmpeg.atPath()
            .addInput(ChannelInput.fromChannel(videoStream))
            .setFilter(StreamType.VIDEO, "crop=w=${rect.width}:h=${rect.height}:x=${rect.xmin}:y=${rect.ymin}")
            .setOverwriteOutput(true)
            .addOutput(ChannelOutput.toChannel("", out).setFormat("ogg"))
            .execute()
        return out
    }

    private fun segmentShape(videoStream: SeekableByteChannel, segmentation: Segmentation): SeekableInMemoryByteChannel {
        val out = SeekableInMemoryByteChannel()

        val images = mutableListOf<BufferedImage>()

        val probe = FFprobe.atPath().setShowStreams(true).setInput(videoStream).execute()
        val videoProbe = probe.streams.first { s -> s.codecType == StreamType.VIDEO }
        val fps = videoProbe.rFrameRate.toInt()

        FFmpeg.atPath()
            .addInput(ChannelInput.fromChannel(videoStream))
            .setOverwriteOutput(true)
            .addOutput(
                FrameOutput.withConsumerAlpha(
                    object : FrameConsumer {
                        override fun consumeStreams(streams: List<Stream?>?) {}

                        override fun consume(frame: Frame?) {
                            if (frame == null) {
                                return
                            }

                            val segmentMask = ImageSegmenter.toBinary(frame.image, segmentation) ?: throw RestErrorStatus(403, "Invalid segmentation")
                            val segmentedImage = ImageSegmenter.segment(frame.image, segmentMask, BufferedImage.TYPE_3BYTE_BGR) ?: throw RestErrorStatus(403, "Invalid segmentation")
                            images.add(segmentedImage)
                        }
                    }
                )
                .disableStream(StreamType.AUDIO)
                .disableStream(StreamType.SUBTITLE)
                .disableStream(StreamType.DATA)
            )
            .execute()

        FFmpeg.atPath()
            .addInput(
                FrameInput.withProducer(
                object : FrameProducer {
                    private var frameCounter = 0
                    override fun produceStreams(): List<Stream> {
                        return listOf(
                            Stream()
                                .setType(Stream.Type.VIDEO)
                                .setTimebase(1000L)
                                .setWidth(images[0].width)
                                .setHeight(images[0].height)
                        )
                    }

                    override fun produce(): Frame? {
                        if (frameCounter >= images.size) {
                            return null
                        }

                        val pts = (frameCounter * 1000 / fps).toLong()
                        val videoFrame = Frame.createVideoFrame(0, pts, images[frameCounter])
                        frameCounter++

                        return videoFrame
                    }
                }
            ))
            .setOverwriteOutput(true)
            .addOutput(ChannelOutput.toChannel("", out).setFormat("ogg"))
            .execute()
        return out
    }

    private fun segmentTime(videoStream: SeekableByteChannel, time: Time): SeekableInMemoryByteChannel? {

        if (time.intervals.size > 1) {
            return null
        }

        val out = SeekableInMemoryByteChannel()
        FFmpeg.atPath()
            .addInput(ChannelInput.fromChannel(videoStream).setPosition(time.intervals[0].first, TimeUnit.SECONDS)
                .setDuration(time.intervals[0].second - time.intervals[0].first, TimeUnit.SECONDS))
            .setOverwriteOutput(true)
            .addOutput(ChannelOutput.toChannel("", out).setFormat("ogg"))
            .execute()
        return out
    }

    private fun segmentChannel(videoStream: SeekableByteChannel, channel: Channel): SeekableInMemoryByteChannel? {

        if (channel.selection.size > 1) {
            return null
        }

        val out = SeekableInMemoryByteChannel()

        val ffmpeg = FFmpeg
            .atPath()
            .addInput(ChannelInput.fromChannel(videoStream))
            .setOverwriteOutput(true)

        when(channel.selection[0]) {
            "video" -> {
                ffmpeg.addArgument("-an")
                    .addOutput(ChannelOutput.toChannel("", out).setFormat("ogg"))
            }

            "audio" -> {
                ffmpeg.addArguments("-q:a", "0").addArguments("-map", "a")
                    .addOutput(ChannelOutput.toChannel("", out).setFormat("ogg"))
            }
        }

        ffmpeg.execute()
        return out
    }
}
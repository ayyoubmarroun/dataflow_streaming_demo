import argparse
from pubsub_stream import PubSubStreamer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pub/Sub purchase demo streamer")
    parser.add_argument("-p", "--project", dest="project_id", type=str, help="GCP project id.")
    parser.add_argument("-s", "--speed", type=int, default=1, help="Speed of streaming")
    parser.add_argument("topic", help="Pub/Sub topic to publish messages to.")

    args = parser.parse_args()

    streamer = PubSubStreamer(project_id=args.project_id, topic=args.topic, speed=args.speed)
    streamer()

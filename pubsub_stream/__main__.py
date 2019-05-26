import argparse
from pubsub_stream import PubSubStreamer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pub/Sub purchase demo streamer")
    parser.add_argument("-p", "--project", dest="project_id", type=str, required=True, help="GCP project id.")
    parser.add_argument("-s", "--speed", type=int, defualt=1, help="Speed of streaming")
    parser.add_argument("topic", required=True, help="Pub/Sub topic to publish messages to.")

    args = parser.parse_args()

    streamer = PubSubStreamer(**args)
    streamer()

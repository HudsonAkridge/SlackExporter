from datetime import datetime
import io
import json
from slackclient import SlackClient

def add_user_info(existing_users_dict, user_id, slack_client):
    if user_id not in existing_users_dict:
        user = slack_client.api_call("users.info", user=user_id)["user"]
        existing_users_dict[user_id] = user["real_name"] if "real_name" in user else user["name"]

def multiple_replace(text, word_dict):
    for key in word_dict:
        text = text.replace(key, word_dict[key])
    return text

def get_timestamp_from_message(msg):
    return datetime.fromtimestamp(float(msg["ts"]))

def write_message(file_pointer, msg, userid_realname_lookup):
    current_timestamp = get_timestamp_from_message(msg)
    username = userid_realname_lookup[msg["user"]]
        #get rid of multiple returns from slack, as well as changing all the userid aliases to real users
    message_text = multiple_replace(str(msg["text"]), userid_realname_lookup).replace("\n\n", "\n")
    line_to_write = "{} ({}): {}\n".format(username, current_timestamp, message_text)
    file_pointer.write(unicode(line_to_write))

def get_channels_list(config, slack_client):
    next_cursor = ""
    while True:
        channel_list_response = slack_client.api_call("channels.list", limit=1000, exclude_members=True, cursor=next_cursor)
        channels = channel_list_response.get("channels", [])

        for channel in channels:
            yield channel

        next_cursor = channel_list_response["response_metadata"].get("next_cursor")
        if not next_cursor:
            return

def get_channel_id(config, slack_client):
    channel_id = config.get("slackChannelId","")
    if channel_id:
        return channel_id

    channel_name = config.get("slackChannelName","").lower()
    if not channel_name:
        raise KeyError("slackChannelName or slackChannelId must be set to a valid value in the config to continue.")

    for channel in get_channels_list(config, slack_client):
        if channel["name"].lower() == channel_name:
            return channel["id"]

    raise ValueError("Channel {0} was not found in the list of channels.".format(channel_name))

def get_channel_messages(slack_client, channel_id):
    epoch = datetime.utcfromtimestamp(0)
    current_oldest_ts = datetime(2000,1,1) #funfact, datetime.min fails when calling .timestamp(), needs to be 1979 or newer.

    while True:
        oldest_ts_queryparameter = (current_oldest_ts - epoch).microseconds * 1000
        channel_history_response = slack_client.api_call("channels.history", channel=channel_id, count=200, oldest=oldest_ts_queryparameter)

        messages = channel_history_response["messages"]

        current_oldest_ts = max(get_timestamp_from_message(messages[0]), current_oldest_ts)

        for message in messages[::-1]:
            #we only care about messages with a user attached to them.  Things
            #without users are attachments, and other crap we don't care about
            if "user" in message:
                yield message

        if not channel_history_response.get("has_more"):
            return

def write_channel_history_to_file(slack_channel_id, slack_client, userid_realname_lookup, file_pointer):
    for message in get_channel_messages(slack_client, slack_channel_id):
        #update user lookup table
        add_user_info(userid_realname_lookup, message["user"], slack_client)
        
        write_message(file_pointer, message, userid_realname_lookup)

def main():
    config = json.load(open("Config.json", "r"))
    slack_client = SlackClient(config["slackToken"])

    userid_realname_lookup = {}
    slack_channel_id = get_channel_id(config, slack_client)

    with io.open("messages.txt", "w", encoding="utf8") as file_pointer:
        write_channel_history_to_file(slack_channel_id, slack_client, userid_realname_lookup, file_pointer)

if __name__ == "__main__":
    main()

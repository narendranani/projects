from slackclient import SlackClient
import time
from slack_assistant import bot

# slack_client = SlackClient("xoxb-402194448037-402195568389-41FVk9DXRbxwlrLKsJHZaS7z")
#
# api_call = slack_client.api_call("users.list")
# if api_call.get('ok'):
#     users = api_call.get('members')
#     for user in users:
#         print(user.get('name'))
#
# if slack_client.rtm_connect(with_team_state=False):
#     print("Successfully connected, listening for events")
#     while True:
#         print(slack_client.rtm_read())
#
#         time.sleep(1)
# else:
#     print("Connection Failed")
print("Narendra")

if __name__ == '__main__':
    b = bot.Bot()
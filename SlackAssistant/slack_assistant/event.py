import command


class Event:
    def __init__(self, bot):
        self.bot = bot
        self.command = command.Command()

    def wait_for_event(self):
        events = self.bot.slack_client.rtm_read()
        print("events: ", events)

        if events and len(events) > 0:
            for event in events:
                # print event
                self.parse_event(event)

    def parse_event(self, event):
        print("event['text']", event)
        print("self.bot.bot_id , event['text']", self.bot.bot_id, event.keys())
        print("event['text']", event.get('text', None))
        if event and 'text' in event and "user" in event:
            # self.handle_event(event['user'], event['text'].split(self.bot.bot_id)[1].strip().lower(), event['channel'])
            self.handle_event(event['user'], event['text'].lower(), event['channel'])
            print("event", event)

    def handle_event(self, user, command, channel):
        print("handel", command)
        if command and channel:
            print("Received command: " + command + " in channel: " + channel + " from user: " + user)
            response = self.command.handle_command(user, command)
            print("response: ", response)
            self.bot.slack_client.api_call("chat.postMessage", channel=channel, text=response, as_user=False)

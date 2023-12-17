# Databricks notebook source
# DBTITLE 1,Install Slack SDK
pip install slack_sdk

# COMMAND ----------

# DBTITLE 1,Import Statements
import os
# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pyspark.sql import Row

# COMMAND ----------

# DBTITLE 1,Define Constants and Variables
# Store conversation history
conversation_history = []
# ID of the channel you want to read the message from
# check ReadMe file to see how you can get the Channel Id in slack.
channel_id = "XXXXXXXXX"  # Specify the slack channel ID
# Store conversation history with replies
conversation_history_replies = []
item_to_bucket_list_map = {}
i=0
outputList =[]
slack_token = "xxxxxxxxxx"   #slack token used for authentication/authorization

# COMMAND ----------

# DBTITLE 1,Define Functions
def set_global_client_variable(token: str):
    """
    Sets the global webclient variable so can be used in different api calls.

    Parameters
    ----------
    token : string
        Token value used for authentication.
    """
    global client
    from slack_sdk import WebClient
    client = WebClient(token=token)

# COMMAND ----------

def get_parent_level_conversations(channel_id: str) -> []:
    """Gets root parent level messages from the slack channel

      Parameters
      ----------
      channel_id : String
          The channel id from which we want to read the messages.

      Returns
      -------
      conversation_history
          A list of the conversation history messages
      """

    # Store conversation history
    conversation_history = []
    try:
        result = client.conversations_history(channel=channel_id)
        conversation_history = result["messages"]
        # Print results
        print("{} messages found in {}".format(len(conversation_history), id))
    except SlackApiError as e:
        print("Error creating conversation: {}".format(e))
    return conversation_history

# COMMAND ----------

def get_conversation_replies_along_with_parent_message(conversation_history: [], channel_id: str) -> {}:
    """Gets a dictionary of replies for all the conversation history list passed to the method

      Parameters
      ----------
      conversation_history: []
          List of the parent root level messages without reply threads.
      channel_id : String
          The channel id from which we want to read the messages.

      Returns
      -------
      item_to_bucket_list_map
          A dictionary of messages along with the replies associated with the specific index.
      """
    item_to_bucket_list_map = {}
    i = 0
    #  conversations_replies gives the latest/last message first and then move to the older messages.
    for history in conversation_history:
        ts_value = history["ts"]
        replies_result = client.conversations_replies(channel=channel_id, ts=ts_value)
        if "subtype" not in replies_result["messages"][0]:  # just to ignore the messages that are present to include the channel join messages
            item_to_bucket_list_map[i] = replies_result["messages"]
            i = i + 1
    return item_to_bucket_list_map

# COMMAND ----------

def get_list_of_instruction_output_messages_dictionary(item_to_bucket_list_map: {}) -> {}:
    """Gets a list of dictionaries with the messages in the output format intended with instruction, context and output as keys in the internal dictionaries.

      Parameters
      ----------
      item_to_bucket_list_map: {}
          A dictionary of messages along with the replies associated with the specific index.

      Returns
      -------
      outputList
          A list of dictionaries with each dictionary having the keys - instruction, context and output.
      """
    outputList = []
    for key in item_to_bucket_list_map:
        #  print(key, 'corresponds to', item_to_bucket_list_map[key])
        #  initializing the instruction and context here so that I can associate instruction and context with all the replies in the output json.
        instruction = ""
        context = ""
        output = ""
        for entry in item_to_bucket_list_map[key]:
            #  If we have just the Parent Message (For example Just Question and no answers/replies at the time of fetching it) then it does not have the attribute thread_ts in the json
            if "thread_ts" in entry:
                if entry['ts'] == entry['thread_ts']:
                    fullMessage = entry["text"]
                    messagesarray = fullMessage.split('Context:')
                    instructionarray = messagesarray[0].split('Q:')
                    #  If the question is asked with the Q: at the start of the text otherwise we can take the text as question.
                    if len(instructionarray) > 1:
                        instruction = instructionarray[1].strip()
                    else:
                        instruction = instructionarray[0].strip()
                    if len(messagesarray) > 1:
                        context = messagesarray[1].strip()
                    else:
                        context = ""
                else:
                    #  we get multiple replies. Here do we need to consider all the replies and how we want to handle that.
                    outputarray = entry['text'].split('A:')
                    if len(outputarray) > 1:
                        output = outputarray[1].strip()
                    else:
                        output = outputarray[0].strip()
            else:
                print("Parent message with no replies")
            if output != "":
                intermediateDictionary = {
                    "instruction": instruction,
                    "context": context,
                    "output": output
                }
                outputList.append(intermediateDictionary)
    return outputList

# COMMAND ----------

#  Call the conversations.history method using the WebClient
#  conversations.history returns the first 100 messages by default
#  These results are paginated, see: https://api.slack.com/methods/conversations.history$pagination
#  conversations.history does not return the replies that we give on the threads.
set_global_client_variable(slack_token)
conversation_history = get_parent_level_conversations(channel_id=channel_id)
messages_dictionary = get_conversation_replies_along_with_parent_message(conversation_history, channel_id)
output_list = get_list_of_instruction_output_messages_dictionary(messages_dictionary)

# COMMAND ----------

output_list

# COMMAND ----------

# DBTITLE 1,Convert to Json File 
# Creating the json file
# here we are creating a json file with one element on one line because if we want to use it to fine tune the existing model then we need the json file in that format.
import json
with open("data.json", 'w') as f:
    for item in outputList:
        f.write(json.dumps(item) + "\n")

# COMMAND ----------

# DBTITLE 1,Convert to Spark dataframe
slack_df = spark.createDataFrame(Row(**x) for x in output_list)
display(slack_df)

import requests

def get_last_telegram_message(bot_token="7575584376:AAGU-_Ih6ZIEj7VPjFtt98I6C0jyOMC_VRI", chat_id=None, from_bot=False):
    if not chat_id:
        chat_id = -1003110619930  # Replace with your chat ID
    
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    params = {
        "offset": -1,  # Get only the latest update
        "limit": 100,
        "timeout": 0
    }
    
    try:
        # Get bot info to identify bot messages
        bot_info_url = f"https://api.telegram.org/bot{bot_token}/getMe"
        bot_response = requests.get(bot_info_url)
        bot_id = bot_response.json().get("result", {}).get("id")
        
        response = requests.get(url, params=params)
        result = response.json()
        
        if response.status_code == 200 and result.get("ok"):
            updates = result.get("result", [])
            
            # Filter messages from the specific chat
            for update in reversed(updates):
                if "message" in update:
                    msg = update["message"]
                    if msg.get("chat", {}).get("id") == chat_id:
                        sender_id = msg.get("from", {}).get("id")
                        
                        # Filter by sender type
                        if from_bot and sender_id == bot_id:
                            # Message from bot
                            message_text = msg.get("text", "")
                            message_date = msg.get("date")
                            sender = "Bot (You)"
                            
                            print(f"✅ Last bot message retrieved from chat {chat_id}")
                            print(f"📩 From: {sender}")
                            print(f"📝 Message: {message_text}")
                            
                            return {
                                "text": message_text,
                                "date": message_date,
                                "sender": sender,
                                "full_message": msg
                            }
                        elif not from_bot and sender_id != bot_id:
                            # Message from users
                            message_text = msg.get("text", "")
                            message_date = msg.get("date")
                            sender = msg.get("from", {}).get("first_name", "Unknown")
                            
                            print(f"✅ Last user message retrieved from chat {chat_id}")
                            print(f"📩 From: {sender}")
                            print(f"📝 Message: {message_text}")
                            
                            return {
                                "text": message_text,
                                "date": message_date,
                                "sender": sender,
                                "full_message": msg
                            }
            
            print(f"⚠️ No messages found in chat {chat_id}")
            return None
        else:
            print(f"❌ Failed to get updates: {result.get('description', 'Unknown error')}")
            return None
            
    except Exception as e:
        print(f"❌ Error retrieving Telegram message: {str(e)}")
        return None

# Example usage
print("Getting last USER message:")
last_user_msg = get_last_telegram_message(from_bot=False)
if last_user_msg:
    print(f"\n📨 Retrieved User Message: {last_user_msg['text']}")

print("\n" + "="*50 + "\n")

print("Getting last BOT message:")
last_bot_msg = get_last_telegram_message(from_bot=True)
if last_bot_msg:
    print(f"\n📨 Retrieved Bot Message: {last_bot_msg['text']}")

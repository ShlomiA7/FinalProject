from telegram.ext import (Updater, CommandHandler, ConversationHandler, MessageHandler,
                          Filters, CallbackContext, CallbackQueryHandler)
from telegram import KeyboardButton, ReplyKeyboardMarkup, Update, InlineKeyboardButton, InlineKeyboardMarkup
from data_source import DataSource
import os
import logging
import sys
import matplotlib.pyplot as plt
import random

print("Bot started.....")
MODE = os.getenv("MODE")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

TOKEN = os.getenv("TOKEN")
DELIVERY_MEN = ['+972542562628', '+972544969475', '+972505852703']
dataSource = DataSource(os.environ.get("DATABASE_URL"))

if MODE == "dev":
    def run():
        logger.info("Start in DEV mode")
        updater.start_polling()
elif MODE == "prod":
    def run():
        logger.info("Start in PROD mode")
        updater.start_webhook(listen="0.0.0.0", port=int(os.environ.get("PORT", "8443")), url_path=TOKEN,
                              webhook_url="https://{}.herokuapp.com/{}".format(os.environ.get("APP_NAME"), TOKEN))
else:
    logger.error("No mode specified!")
    sys.exit(1)


def start_command(update, context):
    buttons = [[KeyboardButton("Order delivery 🛵")], [KeyboardButton("Something else  🤷‍♂")]]
    context.bot.send_message(chat_id=update.effective_chat.id, text="Hello customer What would you like to do?",
                             reply_markup=ReplyKeyboardMarkup(buttons))


def something_else_handler(update, context):
    buttons = [[KeyboardButton("🆕 Undo 🔙")], [KeyboardButton("Call us 📞")]]
    context.bot.send_message(chat_id=update.effective_chat.id, text="You are welcome to visit the restaurant website"
                                                                    "\nhttps://thaichin.co.il/",
                             reply_markup=ReplyKeyboardMarkup(buttons))


def call_handler(update, context):
    buttons = [KeyboardButton("🆕 Undo 🔙")]
    context.bot.send_message(chat_id=update.effective_chat.id, text="Dial the number: 04-953-3333",
                             reply_markup=ReplyKeyboardMarkup(buttons))


def delivery_handler(update: Update, context: CallbackContext):
    """Request a phone number from the user"""
    con_keyboard = [[KeyboardButton(text="Send my phone number 📲", request_contact=True)]]
    update.message.reply_text("Please share your phone number", reply_markup=ReplyKeyboardMarkup(con_keyboard))


def phone_number_handler(update: Update, context: CallbackContext):
    """Location request for delivery"""
    contact = update.effective_message.contact
    if isinstance(contact.last_name, str):
        context.user_data["NAME"] = contact.first_name + " " + contact.last_name
    else:
        context.user_data["NAME"] = contact.first_name

    phone_number = "+" + contact.phone_number
    if len(phone_number) > 13:
        phone_number = phone_number[-13:]
    context.user_data["CLIENT_NUMBER"] = phone_number
    dataSource.new_client(phone_number, context.user_data["NAME"])
    loc_keyboard = [[KeyboardButton(text="Send location 📍", request_location=True)]]
    update.message.reply_text(f"Hey {context.user_data['NAME']} please share your location for delivery"
                              , reply_markup=ReplyKeyboardMarkup(loc_keyboard))


def location_handler(update: Update, context: CallbackContext):
    global DELIVERY_MEN
    context.user_data['DELIVERY_MAN'] = random.choice(DELIVERY_MEN)
    context.user_data["ORDER_NUMBER"] = dataSource.get_last_order() + 1
    dataSource.new_order(context.user_data["ORDER_NUMBER"], '1',
                         context.user_data["CLIENT_NUMBER"], context.user_data['DELIVERY_MAN'])
    context.user_data["DISH_TYPE_KEYBOARD"] = [[KeyboardButton("Your shopping cart is empty 🛒")],
                                               [KeyboardButton("My recommended dishes🙋"),
                                                KeyboardButton("The most favorite🔝")],
                                               [KeyboardButton("🌏 Appetizer🥟"), KeyboardButton("🌏 Soups🍜")],
                                               [KeyboardButton("🌏 Wok mains🥘"), KeyboardButton("🌏 Pad Thai🫕")],
                                               [KeyboardButton("🌏 side dish🍟"),
                                                KeyboardButton("🌏 Crispy Chicken🍗")],
                                               [KeyboardButton("🌏 Noodles🍝"), KeyboardButton("🌏 Salads🥗")],
                                               [KeyboardButton("🌏 Special🥢"), KeyboardButton("🌏 Mains from sea🍤")],
                                               [KeyboardButton("🌏 Sushi🍱"), KeyboardButton("🌏 Sushi Sandwich🍣")]]
    update.message.reply_text(
        f"ok, {context.user_data['NAME']} "
        f"let's choose dishes to order, you are also welcome to browse the menu:\nhttps://thaichin.co.il/menu/#mr-tab-0"
        , reply_markup=ReplyKeyboardMarkup(context.user_data["DISH_TYPE_KEYBOARD"]))


def recommended_handler(update: Update, context: CallbackContext):
    dishes_keyboard = [[KeyboardButton("🔙 Back")]]
    if dataSource.is_client_new(context.user_data["CLIENT_NUMBER"]):
        best_sellers = dataSource.get_favorite_dishes()
        for dish in best_sellers:
            dishes_keyboard_sub = [KeyboardButton("🥡 " + dish)]
            dishes_keyboard.append(dishes_keyboard_sub)
    else:
        reco_dishes = dataSource.get_recommendation_dishes(context.user_data["CLIENT_NUMBER"])
        current_order_dishes = dataSource.get_current_dishes(context.user_data["ORDER_NUMBER"])
        for dish in reco_dishes:
            if dish[:-4] not in current_order_dishes:
                dishes_keyboard_sub = [KeyboardButton("🥡 " + dish)]
                dishes_keyboard.append(dishes_keyboard_sub)
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(dishes_keyboard),
                             text="We hope you like the dishes we have chosen for you ☺️")


def favorite_handler(update: Update, context: CallbackContext):
    dishes_keyboard = [[KeyboardButton("🔙 Back")]]
    best_sellers = dataSource.get_favorite_dishes()
    current_order_dishes = dataSource.get_current_dishes(context.user_data["ORDER_NUMBER"])
    for dish in best_sellers:
        if dish[:-4] not in current_order_dishes:
            dishes_keyboard_sub = [KeyboardButton("🥡 " + dish)]
            dishes_keyboard.append(dishes_keyboard_sub)
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(dishes_keyboard),
                             text="Enjoy our best selling dishes ☺️")


def dish_type_handler(update: Update, context):
    dishes = dataSource.get_dishes(update.message.text[2:-1])
    dishes_keyboard = [[KeyboardButton("🔙 Back")]]
    for dish in dishes:
        dishes_keyboard_sub = [KeyboardButton("🥡 " + dish)]
        dishes_keyboard.append(dishes_keyboard_sub)
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(dishes_keyboard),
                             text="Select " + update.message.text[2:-1])


def selected_dish_handler(update: Update, context: CallbackContext):
    context.user_data["SELECTED_DISH_NAME"] = update.message.text[2:-4]
    photo_name = context.user_data["SELECTED_DISH_NAME"] + ".png"
    context.bot.sendPhoto(update.message.chat_id, photo=open(photo_name, 'rb'))
    buttons = [[InlineKeyboardButton("I don't want this dish ⛔", callback_data='I dont want this dish ⛔')],
               [InlineKeyboardButton("1", callback_data='1'), InlineKeyboardButton("2", callback_data='2'),
                InlineKeyboardButton("3", callback_data='3')]]
    replay_markup = InlineKeyboardMarkup(buttons)
    update.message.reply_text("How many units would you like of this dish?", reply_markup=replay_markup)


def quantity_handler(update: Update, context: CallbackContext):
    if get_chosen_dish(context):
        return
    context.user_data["chosen " + context.user_data["SELECTED_DISH_NAME"]] = "true"
    query = update.callback_query.data
    update.callback_query.answer()
    dish_number = dataSource.get_dish_number(context.user_data["SELECTED_DISH_NAME"])
    update.callback_query.edit_message_reply_markup(None)
    update.callback_query.answer()
    if "1" in query or "2" in query or "3" in query:
        quantity = int(query)
        dataSource.new_dish_in_order(context.user_data["ORDER_NUMBER"], dish_number, quantity)


def get_chosen_dish(context):
    try:
        return context.user_data["chosen " + context.user_data["SELECTED_DISH_NAME"]]
    except KeyError:
        return None


def back_handler(update: Update, context: CallbackContext):
    context.user_data["SUM"] = dataSource.get_sum_price(context.user_data["ORDER_NUMBER"])
    if type(context.user_data["SUM"]) == int:
        context.user_data["DISH_TYPE_KEYBOARD"][0] = [
            KeyboardButton(f"🛒Shopping cart ({str(context.user_data['SUM'])} ₪)")]
    update.message.reply_text("take your time 😊",
                              reply_markup=ReplyKeyboardMarkup(context.user_data["DISH_TYPE_KEYBOARD"]))


def shopping_cast_handler(update: Update, context: CallbackContext):
    reco_dishes = dataSource.get_recommendation_dishes(context.user_data["CLIENT_NUMBER"])
    dishes_keyboard = [[KeyboardButton("🔙 Back"), KeyboardButton("🛍️ continue ")]]
    current_order_dishes = dataSource.get_current_dishes(context.user_data["ORDER_NUMBER"])
    for dish in reco_dishes:
        if dish[:-4] not in current_order_dishes:
            dishes_keyboard_sub = [KeyboardButton("🥡 " + dish)]
            dishes_keyboard.append(dishes_keyboard_sub)
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(dishes_keyboard),
                             text="We've found some dishes you'll really love, would you like to add to the order?")


def continue_handler(update: Update, context: CallbackContext):
    keyboard = [[KeyboardButton("🔙 Back"), KeyboardButton("✍️i have a remarks")],
                [KeyboardButton("💳 Go to payment")],
                [KeyboardButton("I want to delete one or more dishes from the order🪚")]]
    name = context.user_data["NAME"]
    context.user_data["SUM"] = dataSource.get_sum_price(context.user_data["ORDER_NUMBER"])
    sum_total = context.user_data["SUM"]
    current_order_dishes = dataSource.get_current_dishes(context.user_data["ORDER_NUMBER"])
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(keyboard),
                             text=f"Ok, {name}\nYour order cost {sum_total} ₪ and includes:")
    for i in range(0, len(current_order_dishes), 2):
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'{str(current_order_dishes[i + 1])}\t{str(current_order_dishes[i])}')
    remark = dataSource.get_remark(context.user_data["ORDER_NUMBER"])
    if isinstance(remark, str):
        context.bot.send_message(chat_id=update.effective_chat.id, text=remark)


def remarks_handler(update: Update, context: CallbackContext):
    button = [[KeyboardButton("🛍️ continue ")]]
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(button),
                             text="Write down your remarks, at the end of the message write down that emoji: 📝")


def done_command(update: Update, context: CallbackContext):
    remark = update.message.text
    dataSource.set_remarks(remark, context.user_data["ORDER_NUMBER"])
    context.bot.send_message(chat_id=update.effective_chat.id,
                             reply_markup=ReplyKeyboardMarkup([[KeyboardButton("🛍️ continue ")]]),
                             text="Your comment has been successfully registered")


def delete_dish_handler(update: Update, context: CallbackContext):
    keyboard = [[KeyboardButton("🔙 Back")]]
    current_order_dishes = dataSource.get_current_dishes(context.user_data["ORDER_NUMBER"])
    for i in range(0, len(current_order_dishes), 2):
        dishes_keyboard_sub = [KeyboardButton("❌delete\t" + current_order_dishes[i])]
        keyboard.append(dishes_keyboard_sub)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Select the dishes you want to remove from the order",
                             reply_markup=ReplyKeyboardMarkup(keyboard))


def deleted_handler(update: Update, context: CallbackContext):
    current_order_dishes = dataSource.get_current_dishes(context.user_data["ORDER_NUMBER"])
    dish_to_delete = update.message.text[8:]
    if dish_to_delete in current_order_dishes:
        dish_number = dataSource.get_dish_number(dish_to_delete)
        dataSource.delete_dish_from_order(dish_number, context.user_data["ORDER_NUMBER"])
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f"{dish_to_delete} has been removed from your order")
    else:
        context.bot.send_message(chat_id=update.effective_chat.id, text=f"{dish_to_delete} is no longer in your order")


def payment_handler(update: Update, context: CallbackContext):
    keyboard = [[KeyboardButton("🔙 Back")], [KeyboardButton("♦ Apple Pay 🍏"), KeyboardButton("♦ Credit Card 💳")],
                [KeyboardButton("♦ Cash 💷"), KeyboardButton("♦ Bit 🟦")]]
    context.bot.send_message(chat_id=update.effective_chat.id, reply_markup=ReplyKeyboardMarkup(keyboard),
                             text="How would you like to pay?")


def finish_handler(update: Update, context: CallbackContext):
    keyboard = [[KeyboardButton("🆕 Make new order 🥳")], [KeyboardButton("What is the status of my shipment?")]]
    delivery_person_name = dataSource.get_delivery_person(context.user_data["ORDER_NUMBER"])[0]
    delivery_person_number = dataSource.get_delivery_person(context.user_data["ORDER_NUMBER"])[1]
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f"Excellent!, we have started working on your order and it will be out soon\n"
                                  f"your delivery man is {delivery_person_name}\n and his phone number is:"
                                  f" {delivery_person_number}",
                             reply_markup=ReplyKeyboardMarkup(keyboard))


def bos_command(update, context):
    keyboard = [[KeyboardButton("🆕")],
                [KeyboardButton("Show Income chart from the last week📊")],
                [KeyboardButton("Show distribution of income per dishes types📊")],
                [KeyboardButton("The dishes that brought in the least money this month📉")],
                [KeyboardButton("The dishes that brought in the most money this month📈")]]
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Hey Or",
                             reply_markup=ReplyKeyboardMarkup(keyboard))


def weakest_handler(update, context):
    text = "the weakest dishes this month are:"
    dishes = dataSource.get_less_seal_dishes()
    for dish in dishes:
        text += "\n\n" + dish
    context.bot.send_message(chat_id=update.effective_chat.id, text=text)


def best_handler(update, context):
    text = "The best selling dishes this month are:"
    dishes = dataSource.get_best_seal_dishes()
    for dish in dishes:
        text += "\n\n" + dish
    context.bot.send_message(chat_id=update.effective_chat.id, text=text)


def income_dish_type_handler(update: Update, context: CallbackContext):
    dish_type_df = dataSource.get_dish_type_income_df()
    dish_type_df.plot(x='dish_type', y='income', kind='bar')
    plt.savefig('dish_type_bar.png')
    context.bot.send_photo(chat_id=update.effective_chat.id, photo=open('dish_type_bar.png', 'rb'))


def weekly_income_handler(update: Update, context: CallbackContext):
    week_income_df = dataSource.get_last_week_income_df()
    week_income_df.plot(x='order_time', y='daily_income', kind='bar')
    plt.savefig('weekly_income_bar.png')
    context.bot.send_photo(chat_id=update.effective_chat.id, photo=open('weekly_income_bar.png', 'rb'))


if __name__ == '__main__':
    updater = Updater(TOKEN, use_context=True)
    updater.dispatcher.add_handler(CommandHandler("start", start_command))
    updater.dispatcher.add_handler(CommandHandler("mypassword", bos_command))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Order delivery 🛵'), delivery_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.contact, phone_number_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.location, location_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("🌏"), dish_type_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("🥡"), selected_dish_handler))
    updater.dispatcher.add_handler(CallbackQueryHandler(quantity_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("🔙 Back"), back_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("🛒Shopping cart"), shopping_cast_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("🛍️ continue"), continue_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("💳 Go to payment"), payment_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("♦"), finish_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("🆕"), start_command))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("My recommended dishes🙋"), recommended_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("The most favorite🔝"), favorite_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("Something else  🤷‍♂"), something_else_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("The dishes that brought in the least money this "
                                                                "month📉"),
                                                  weakest_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("The dishes that brought in the most money this"
                                                                " month📈"), best_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("I want to delete one or more dishes from the "
                                                                "order🪚"), delete_dish_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("❌delete"), deleted_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("✍️i have a remarks"), remarks_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("📝"), done_command))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("Show Income chart from the last week📊"),
                                                  weekly_income_handler))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex("Show distribution of income per dishes types📊"),
                                                  income_dish_type_handler))
    run()

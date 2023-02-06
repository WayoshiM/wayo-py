# wayo-py
My personal hobby project. In most online circles I go by "Wayoshi", a portmanteau of "Waluigi" and "Yoshi" from the Super Mario video game series I coined up one day in 2006 as a username to go with that sounded good enough.

[Discord](https://discord.com) is a popular chat service, effectively replacing Internet Relay Chat (IRC) of the 90s/00s as the go-to chatting application. It is very easy to set up a "server" for all types of communities.

One hobby I am very passionate about is game shows. I am an avid fan of The Price is Right and Wheel of Fortune, and really enjoy the statistics that can come out of analyzing the sub-games played within an episode (for Price is Right) or the Hangman-type puzzles used (Wheel of Fortune). To that end, I've created a Discord bot that builds and maintains these databases, and allows users to query it in a variety of ways, among some other smaller functionalities.

The project name "wayo.py" just comes from my nickname of a nickname many call me, "Wayoshi" --> "Wayo", plus adding a `.py` extension. This is written in 100% pure Python, using the [discord.py](https://github.com/Rapptz/discord.py) library to interface with all the messy Discord endpoints, among other helpful libraries.

The databases were originally maintained in [Pandas](https://pandas.pydata.org). The data size of roughly 10000 rows by 25 columns for the Price is Right data and 45000 rows by 12 columns for Wheel of Fortune data should not cause users to encounter any memory issues, and while the data size continues to grow with new episodes, it is not at a fast rate compared to what exists. However, when coding up allowing the end users to search with more and more complicated criteria, the performance of those queries was quite slow at times. This has led me to refacoring the database code to [Polars](https://github.com/pola-rs/polars), an exciting new library in the past year+ that takes advantage of its Rust backend to optimize queries by passing Python's Global Interpreter Lock (GIL). I also feel its syntax is much more readable and flows better than Pandas.

The code here is a broad sampling of the most important parts of the bot, along with two FAQ markdown files for the most complicated parts. Any passwords / access tokens have been redacted. There are two FAQs for end users on how to use the most complicated parts.

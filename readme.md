# FA search bot
This is a telegram bot which can:
- Neaten up FA links into photos/documents, with captions.
- Neaten up FA links by direct file link.
- Inline FA searches. Also gallery, scraps and favourites browsing.
- Subscribe to queries on FA, and receive notifications when new matching results are posted.

Future feature plans:
- FA notifications

You can use it freely at [@FASearchBot](https://t.me/FASearchBot), add it to your own chats and channels, or use it inline anywhere by mentioning it

## Commands

### Neaten

Private message it a link to a Fur Affinity submission page, thumbnail url or image download url, i.e, a link 
beginning with `https://d.furaffinity.net/art/` or `https://t.furaffinity.net/` 
and it will reply with the image embedded and a link to that image's gallery page.
If you send a gif link, it will convert it for optimal quality in telegram.

### Inline Responses

In any chat, mention the bot and pass it a Fur Affinity search query
`@FASearchBot [query]` and pick from the inline results which image you'd like to share.

You can also use these qualifiers inline:
- `@FASearchBot gallery:username`
- `@FASearchBot scraps:username`
- `@FASearchBot favs:username`

#### Inline Search Queries

The bot will support all Fur Affinity queries, and some extra filter options:

Taken from FurAffinity's search documentation: 
> Search understands basic boolean operators:
> * AND: `hello & world`
> * OR : `hello | world`
> * NOT: `hello -world` -or- `hello !world`
> 
> Grouping: `(hello world)`
> Example: `( cat -dog ) | ( cat -mouse)`
>
> *Capabilities*
> Field searching: `@title hello @message world`
> Phrase searching: `"hello world"`
> Word proximity searching: `"hello world"~10`
> Quorum matching: `"the world is a wonderful place"/3`
> Example: `"hello world" @title "example program"~5 @message python -(php|perl)`
> 
> Available Fields: `@title` `@message` `@filename` `@lower` (artist name as it appears in their userpage URL) `@keywords`
> Example: `fender @title fender -dragoneer -ferrox @message -rednef -dragoneer`

### Subscriptions

- `/add_subscription [query]`
- `/list_subscriptions`
- `/remove subscription [query]`

- `/add_block [tag]` Add a new query to the block list
- `/list_blocks` List all blocked tags and queries in this chat
- `/remove_block [tag]` Remove a blocked query from the list for this chat

- `/pause [query?]` - Pauses all subscriptions in chat, or specified subscription
- `/resume [query?]` - Resumes all subscriptions in chat, or specified subscription

#### Subscription queries

Subscriptions understand basic boolean operators:
- `-`, `!`, or `not` to exclude from the results, e.g. `taur -ych -rating:adult`
- `and` and `or` to combine options, e.g. `taur or centaur`
- You can use brackets too, e.g. `(taur or centaur) and not ych`

Phrases can be specified with quotation marks, e.g. `"open ych" -reminder`

And fields can be specified:
- `keywords:deer` (or `tags:deer`, `@keyword deer`)
- `title:free` (or `@title free`)
- `description:dragoness` (or `message: dragoness`)
- `artist:rajii` (or `author:rajii`, `uploader:rajii`, `poster:rajii`, `@lower rajii`, etc)
- `rating:general` (or `rating:safe`)
- `rating:mature` (or `rating:questionable`)
- `rating:adult` (or `rating:explicit`)

You can also combine fields with other operators, for example using one of the negation operators
 to exclude from the results, e.g. `taur -ych -rating:adult`

Words can also be given with asterisks to allow prefixes or suffixes, e.g. `multi*`, `*taur`

Words (particularly ones with asterisks) can have an "except" or "ignore" clause, 
which can be a word, a quote, or a bracketed list of words/quotes, 
e.g. `taurs except "no taurs"`, `multi* ignore (multitude or multicolour*)`  
Submissions matching the original word, only in the case of the ignored words, will then not be included.
But submissions that match both separately will be included. 
For example `multi* ignore multicolour*` would still match `multicoloured multi-armed friend`.

Block queries use the same syntax, and are combined with queries like so: `(query) and not (blocklist)`

A full [railroad/syntax diagram](https://en.wikipedia.org/wiki/Syntax_diagram) of the query syntax is available here: 
[query_parser_railroad.html](docs/query_parser_railroad.html)

### Miscellaneous

`/beep` - responds with `boop`

Send or forward the bot an image with no text as a private message, and it will recommend you try 
[@foxbot](https://t.me/foxbot) ([open source](https://github.com/Syfaro/foxbot)) or [@FindFurryPicBot](https://t.me/FindFurryPicBot) (closed source)

## Development 

### Running the bot for yourself

To set this up, you'll need to:

 - Download or clone the repo
 - Install requirements: `pip install -r requirements.txt`
 - Add a `config.json` in the base directory, like so:

```json
{
  "bot_key": "---",
  "api_url": "https://faexport.spangle.org.uk"
}
```

- Set `bot_key` to your telegram bot API key, and `api_url` to the URL of a valid deployment of 
[the FA API](https://github.com/Deer-Spangle/faexport).
 - Run `python3 run.py`

If you want it to be able to prettify gifs, you will need docker running on the system.

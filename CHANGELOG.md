# Changelog

Changelog for FASearchBot, should include entries for these types of changes:

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities. Format inspired by https://keepachangelog.com/en/1.0.0/

## [1.16.0] - 2025-08-01

### Changed
- Split MediaFetcher into MediaDownloader and MediaUploader
- Various tweaks and performance improvements
- Lots and lots of internal improvements

### Added
- Lots of new metrics and logs
- Added short commands for adding/listing/removing block queries

## [1.15.25] - 2025-06-09

### Fixed

- Improved logging and reset when SubWatcher sender fails to send submission due to file part missing error
- Set a cap on the upload queue, to avoid stale data when the media uploader is running slower than the data fetcher while the script is catching up a large backlog

## [1.15.24] - 2025-06-04

### Fixed

- If the media fetcher dies trying to upload a submission, re-fetch the data before trying to upload it again

## [1.15.23] - 2025-05-28

### Fixed

- If sending a submission from a new cache entry in subscription watcher, save that cache entry to the submission in the wait pool

## [1.15.22] - 2025-05-28

### Fixed

- If subscription sender receives FilePartMissingError from Telegram, reset the media reference and try upload again
- If the subscription sender has to re-try sending a submission, don't re-send it to places it was successfully sent

## [1.15.21] - 2025-05-08

### Fixed

- Fixed bug where subscription watcher would send 1 submission without progressing further
- Fixed import error

## [1.15.20] - 2025-05-08

### Changed

- When sender encounters an error, don't ignore it
- If sender received a FloodWaitError, wait before attempting new send

## [1.15.19] - 2025-04-22

### Fixed

- Fix circular import which caused startup failure

## [1.15.18] - 2025-04-22

### Fixed

- If a runnable task in the subscription watcher fails, allow it to revert the last attempt, before restarting

## [1.15.17] - 2025-04-17

### Changed

- If a runnable task in the subscription watcher fails, just restart and continue, rather than shutting down

## [1.15.16] - 2025-03-18

### Fixed

- If FA returns 524 error on download, retry the download
- Handle ClientOSError exceptions in media fetcher

## [1.15.15] - 2024-11-07

### Fixed

- Set a user agent when making requests to Weasyl

## [1.15.14] - 2024-07-03

### Added

- Handle fxraffinity.net links as if they're furaffinity.net ones

## [1.15.13] - 2023-12-14

### Fixed

- Handling http 522 status code when attempting to download media in the media fetcher, and retrying after a short wait

## [1.15.12] - 2023-09-22

### Fixed

- Media fetcher will now correctly handle http 520 and 403 responses from FA CDN
- Subscription watcher will now give up if it has been unsuccessful at fetching media for a submission over 100 times.
- Fixing a bodged refactor and putting the query parser back inside fa_search_bot

## [1.15.11] - 2023-09-08

### Fixed

- If subscription watcher gets 502 error while downloading media, backoff and try again

## [1.15.10] - 2023-09-03

### Fixed

- If a story submission lacks a preview image, try using the thumbnail image instead. This should avoid the subscription
  watcher ending up in a cycle passing a submission back and forth between data fetcher and media fetcher.

## [1.15.9] - 2023-08-31

### Changed

- Instead of skipping submissions when the media returns 404, the media fetcher will push them back to the data fetcher
  to fetch the data again, in case the submission's media URL has changed, rather than the submission being deleted.

## [1.15.8] - 2023-08-31

### Fixed

- Correctly handle 404 during submission upload in the subscription watcher

## [1.15.7] - 2023-08-31

### Changed

- Raise exception if unexpected status code received while downloading image. (May lower reliability until we find out
  where these exceptions get raised)

## [1.15.6] - 2023-08-24

### Changed

- Increase reliability of neaten functionality, by making it retry after cloudflare/slowdown errors a few times

## [1.15.5] - 2023-08-24

### Changed

- Detect 429 responses from FAExport, as well as 503 ones. Also use error_type for error identification

## [1.15.4] - 2023-08-16

### Changed

- If an image won't load, or save, in PIL, attempt to load it with the LOAD_TRUNCATED_IMAGES flag set.

## [1.15.3] - 2023-07-27

### Changed

- Change `fasearchbot_fasubwatcher_latest_posted_at_unixtime` metric to record the FA posted timestamp of the last
  submission sent by the subscription Sender, rather than the last one whose data was fetched by the DataFetcher.
- MediaFetcher will attempt to re-upload to Telegram if it gets a ClientPayloadError during upload

## [1.15.2] - 2023-07-24

### Added

- Added the ability for the bot to lookup some submissions by filename, allowing automatically embedded FA gifs to be
  optimised, or downloaded e621 files to be looked up.

## [1.15.1] - 2023-07-21

### Fixed

- Fixed slow heartbeat from SubIDGatherer which was leading to excess alerts.
- Fixed race condition where submissions could be delivered to deleted subscriptions if they were still in the queue.

## [1.15.0] - 2023-07-19

### Changed

- Huge rewrite of the logic of the subscription watcher. Instead of doing everything sequentially, it now has a 4 part
  process of different tasks passing data across in queues. First task gathers new submission IDs, second task fetches
  the data for those IDs and checks subscriptions, third task downloads media and uploads to telegram, fourth task sends
  telegram messages. The fourth task always ensures messages are sent to telegram in the order they are published to FA,
  while allowing the second and third tasks to be scaled out to run in parallel to multiple copies of eachother. This
  should improve throughput a fair bit!
- FAExportAPI web requests are all async now, rather than blocking.
- Metrics will have changed a lot in the fasearchbot_fasubwatcher_* area.
- Four heartbeats, one for each task.
- If a submission fails to process, the subscription watcher shuts down rather than skipping one. Hopefully it shouldn't
  fail to process!

## [1.14.1] - 2023-07-06

### Changed

- If FA browse page is unavailable for the subscription watcher (such as during cloudflare protection), try using the
  home page, as we only want the latest submission ID anyway.

## [1.14.0] - 2023-06-08

### Added

- Added support for neatening and embedding Weasyl submission links

### Fixed

- Correctly record metrics for how long it takes to upload audio and image submissions, rather than just videos

## [1.13.9] - 2023-05-15

### Fixed

- Correctly handling submission links which have been deleted from e621
- Removed call to `str.removeprefix()` which caused issues with older python versions

## [1.13.8] - 2023-05-15

### Fixed

- Fixed two pass video conversion

### Added

- Added thumbnails to sent videos

## [1.13.7] - 2023-05-14

### Fixed

- Fixed bug where inline search might report no results exist at the end of a search
- Fixed bug where submissions sent by fallback methods would be cached
- Only download images once when sending them, hopefully speeding things up

### Changed

- Setting better filenames on videos and gifs (with site and submission ID)
- Setting title and performer on audio files sent by the bot
- Separating the methods for uploading media to telegram and sending it as a message, allowing future performance
  improvements

## [1.13.6] - 2023-05-06

### Fixed

- Fixed bug where gifs could not be sent via inline neatening. (Fixes sending audio and pdf also)

### Changed

- When sending submissions that have already been cached as full results, inline result will be sent correctly first
  time, rather than having an optimise button and calling inline edit.

## [1.13.5] - 2023-04-28

### Added

- Added metrics on time taken by various steps of sending a fresh message, `fasearchbot_sendable_time_taken`

## [1.13.4] - 2023-04-27

### Added

- Added metrics on cache database size, `fasearchbot_db_cache_entries`
- Added metrics on saving and loading cache, cache hits and misses, to submission cache, `fasearchbot_submissioncache_*`
- Added metrics on inline query result counts, cached and fresh, `fasearchbot_inline_*_results_count`

## [1.13.3] - 2023-04-27

### Fixed

- Improved reliability of inline search, returning smaller batches of fresh results for speed, but sending larger 
  batches of cached results if available.

## [1.13.2] - 2023-04-24

### Added

- Added new metric, `fasearchbot_sentsubmission_sent_from_cache_total` storing how many messages are sent from cache.

### Fixed

- Subscription watcher to handle PeerIdInvalidError by pausing all subscriptions to that peer.

## [1.13.1] - 2023-04-24

### Fixed

- Subscription watcher will now send from cache correctly

## [1.13.0] - 2023-04-24

### Added

- Added a cache database that stores the file ID of all images sent in telegram, so that they can be sent much faster 
  the second time. This also came with a big refactor of the internals and site handling and such. Should make inline
  queries more reliable too! (By virtue of being faster)

## [1.12.6] - 2023-04-20

### Added

- Added a prometheus metric `fasearchbot_fasubwatcher_time_taken`, which is logging the time taken by the subscription 
  watcher performing various tasks. This merges the metric series which were added in version 1.12.5.

### Removed

- Removed the older metric series of the form `fasearchbot_fasubwatcher_time_taken_*`, to merge them all into one metric
  series with individual labels for different tasks

## [1.12.5] - 2023-04-18

### Added

- Added a series of prometheus metrics to the subscription watcher, logging the time taken by various stages of the
  subscription watcher's process. These are all of the form `fasearchbot_fasubwatcher_time_taken_*`

## [1.12.4] - 2023-04-04

### Added

- Added a prometheus metric `fasearchbot_fasubwatcher_latest_posted_at_unixtime`, which is the unix timestamp that the 
  latest processed submission was uploaded to FA

### Removed

- Removed the `fasearchbot_fasubwatcher_backlog_seconds` metric, which may become outdated or misleading

## [1.12.3] - 2023-04-04

### Added

- Added a prometheus metric `fasearchbot_fasubwatcher_backlog_seconds`, which says how many seconds old the latest 
  submission checked by the subscription watcher is. (i.e. how many seconds since it was posted on FA)

## [1.12.2] - 2022-11-24

### Added

- Added a prometheus metric `fasearchbot_fasubwatcher_subscription_destination_count_active`, which says how many
  destinations have at least one active subscription

## [1.12.1] - 2022-11-24

### Fixed

- Subscriptions will now be paused when the bot cannot access a channel

## [1.12.0] - 2021-12-02

### Added

- Ability to do inline e621 searches

### Changed

- Split apart inline functionality classes, for easier modification of gallery, search, and favourite inline queries

## [1.11.10] - 2021-11-27

### Security

- Updating direct indirect dependencies to fix security alerts. I don't believe any of these would pose serious security
  issues for this project.

## [1.11.9] - 2021-11-24

### Added

- Adding `g:` shortcode for inline user gallery searches
- Adding `f:` shortcode for inline user favourite searches

## [1.11.8] - 2021-11-23

### Added

- Add support for neatening e621 thumbnail links

## [1.11.7] - 2021-10-11

### Added

- Add support for e926 links

## [1.11.6] - 2021-09-25

### Changed

- Subscription terms which don't specify a field can now match the artist name

## [1.11.5] - 2021-08-19

### Added

- Add `fasearchbot_log_messages_total` metrics on log line counts by level

## [1.11.4] - 2021-08-19

### Added

- Added `fasearchbot_faapi_latest_id` metric, keeping note on the latest FA submission ID

### Changed

- Renamed `fasearchbot_fasubwatcher_updates_failed` metric to `fasearchbot_fasubwatcher_updates_failed_total`

### Fixed

- Fixing `fasearchbot_faapi_cloudflare_errors_total` metric, which had incorrect label naming

## [1.11.3] - 2021-08-18

### Added

- Lots of prometheus metrics on:
  - Running bot version and start up time
  - Functionality usage
  - API timings and error rates
  - Whether FA is in slow mode
  - Sent message counts and types
  - Gif and video conversion timings, error rates, and whether one or two pass conversion was required
  - Video types, lengths, and whether they have audio
  - Docker timings and error rates
  - Submissions processed by subscription handler, error rates and types, and rates of submissions which match
    subscriptions
  - Number of subscriptions and block lists, and destinations those are configured for
  - Backlog of submissions which need checking against subscriptions, and what time the last one was checked
- Added `prometheus_port` option to config to specify which port to run the metrics endpoint on, defaulting to 7065
- Supporting MD5 hashes as IDs for e621 submissions

### Removed

- Removed the usage logger at `logs-usage.log` (in favour of prometheus metrics)

## [1.11.2] - 2021-06-22

### Added

- Added e621 configuration to the config file. Set `e621` to a dictionary containing `username` and `password` keys.
  - This is so that the bot can access e621 posts which are not available to guests

### Changed

- Stop subscription watcher faster when told to stop (after the next submission, rather than after the next batch)
- Convert videos without audio which are over 40 seconds into videos, not gifs

### Fixed

- Handle images lacking an "is_animated" attribute, in case an image is named with the wrong file extension

## [1.11.1] - 2021-06-20

### Added

- Adding notes about e621 posts to welcome message

### Fixed

- Improve railroad diagram generation script
- Fixed two-pass video conversion such that it accommodates audio bitrate in its calculation of target video bitrate so
  that resulting videos are not above telegram size limits

## [1.11.0] - 2021-06-20

### Added

- Added support for e621
  - Reworking a lot of functionalities to handle e621 links
  - Creating Sendable to provide generic ways for functionalities to send submissions from different sites

### Fixed

- Fixed version number parsing to be a little less fragile

## [1.10.7] - 2021-06-12

### Changed

- In groups, neaten functionality should ignore gif and video captions, as well as image captions

## [1.10.6] - 2021-06-10

### Changed

- If a destination is removed, or the bot is blocked from it, pause all subscriptions to that destination

## [1.10.5] - 2021-06-10

### Changed

- Edit inline query messages after they are sent, to raise the quality level

## [1.10.4] - 2021-06-06

### Fixed

- Fixed loading of subscription data

## [1.10.3] - 2021-06-06

### Changed

- If subscription data file is empty, treat it as if it does not exist

## [1.10.2] - 2021-06-04

### Changed

- Adding title and author to non-image submissions (such as stories and music)
- Convert static gifs to png, so that telegram doesn't treat them as animated
- Separate github actions job for unit tests from telegram integration tests
- Creating website integration tests folder, for cases where we want to ensure a website behaves the same as we have
  assumed

## [1.10.1] - 2021-05-31

### Changed

- Handle gif conversions asynchronously, so it does not block all other actions

## [1.10.0] - 2021-05-30

### Added

- Added the ability to send individual submissions by passing the link or ID to an inline query

### Changed

- Creating generic SiteHandler base, and FAHandler implementation, laying the groundwork for supporting other websites

### Removed

- Removed no longer used python-telegram-bot dependency

## [1.9.2] - 2021-05-27

### Fixed

- Neaten now works in megagroups as well as groups

## [1.9.1] - 2021-05-25

### Fixed

- Fixed gallery display in inline photo query on mobile

## [1.9.0] - 2021-05-25

### Changed

- Switch from python-telegram-bot to Telethon
  - Allowing asynchronous operation
  - Better handling of flood limits
  - Better handling of errors when sending messages
- Switch subscription watcher from a separate thread, to an asynchronous task

## [1.8.10] - 2021-03-06

### Fixed

- No longer ignoring images with url buttons and no caption

## [1.8.9] - 2021-03-05

### Fixed

- Better handling and clarity around cloudflare errors

## [1.8.8] - 2021-03-03

### Fixed

- Fixed handling of messages without URL buttons

## [1.8.7] - 2021-03-03

### Changed

- Recommending @reverseSearchBot for looking up sources of gifs
- Checking URL buttons for links when messages are sent to the bot for neatening
- Replying to the unhandled message when warning that a message is unhandled

## [1.8.6] - 2021-02-23

### Changed

- No longer considering hypens and underscores to be word boundaries when checking subscription matches
- Only log usage of image hash recommend functionality in private message

## [1.8.5] - 2021-02-12

### Changed

- Switch from travis CI to github actions

### Fixed

- Switch to handling furaffinity.net direct and thumbnail links, as well as the old facdn.net links

## [1.8.4] - 2021-02-06

### Fixed

- Handling FA submissions without thumbnails

## [1.8.3] - 2021-02-06

### Changed

- Improved logging for deleted submissions in subscription watcher

## [1.8.2] - 2021-01-13

### Fixed

- Checking all submission IDs, rather than just the ones on browse listings, so that we don't miss scraps

## [1.8.1] - 2021-01-13

### Added

- Telegram integration tests running a full copy of the bot at a staging handle and sending messages to it
- Added notes on pause and resume functionality to readme

### Fixed

- Shutdown process closes all threads
- Fixed error where migrated chat would not recognise duplicate subscriptions

## [1.8.0] - 2020-11-04

### Added

- CodeQL static analysis

### Changed

- Subscriptions can now be configured in channels
- Requirements and tasks are now managed via poetry, rather than requirements.txt anbd makefile

## [1.7.6] - 2020-10-30

### Fixed

- Will no longer try and neaten links in channels, or links from image captions in groups

## [1.7.5] - 2020-09-28

### Added

- Experiment results on maximum telegram gif resolutions

### Changed

- Increased the maximum size of gifs to 1280x1280, (from 720x720)

## [1.7.4] - 2020-09-15

### Fixed

- Now supporting new d2.facdn and t2.facdn direct links

## [1.7.3] - 2020-09-10

### Added

- Experiment on maximum telegram image resolutions

### Fixed

- Made sending photos a bit more resilient, as random URLs seem to fail to embed in telegram bot API

## [1.7.2] - 2020-09-07

### Changed

- Checking registered users count on FA, and slowing down if site is in slowdown mode

## [1.7.1] - 2020-08-12

### Fixed

- Migrate chat subscriptions if a group is upgraded to supergroup

## [1.7.0] - 2020-08-07

### Added

- Added `except` keyword to subscription queries, such that wildcard queries can have exceptions they do not match

## [1.6.7] - 2020-08-05

### Changed

- Moved run.py out of the package and into the root directory

## [1.6.6] - 2020-08-05

### Added

- Added lots of logging and usage logging

### Changed

- Extracted MQBot class into a separate file

## [1.6.5] - 2020-07-29

### Fixed

- Message formatting errors for subscriptions with underscores, by switching from markdown to html message formatting

## [1.6.4] - 2020-07-29

### Changed

- Moved everything into fa_search_bot package

## [1.6.3] - 2020-07-29

### Changed

- Added link to FoxBot source code in readme

### Fixed

- Better handling of large images so that if they fail to send, a smaller version will be sent

## [1.6.2] - 2020-07-25

### Added

- Bot now sends prettier and better telegram-optimised gifs by using dockerised ffmpeg to convert them to mp4 files
- Funding link: ko-fi

## [1.6.1] - 2020-07-19

### Changed

- Making prefix and suffix subscription queries not match if the whole word is equal to the specified prefix/suffix

## [1.6.0] - 2020-07-19

### Added

- Complex subscription query parsing, with and/or, fields, phrases, ratings, prefix/suffix, and all sorts
- Added an experiment on doing this with whoosh, but ended up abandoning that and using pyparsing and custom matching

## [1.5.4] - 2020-07-14

### Changed

- If a submission matches multiple subscriptions in one chat, only one message is sent, with the list of matching
  subscriptions

## [1.5.3] - 2020-07-14

### Changed

- Added @FoxBot to the list of recommended bots to locate image sources, alongside @FindFurryPicBot

## [1.5.2] - 2020-07-14

### Added

- Added "!" as a negation modifier, alongside "-", for subscriptions

## [1.5.1] - 2020-07-13

### Fixed

- Allow subscriptions to be configured in channels

## [1.5.0] - 2020-07-13

### Added

- Subscriptions can now specify the rating of submissions

### Changed

- Renaming block list functionality
- Default to faexport.spangle.org.uk rather than faexport.boothale.net

## [1.4.3] - 2020-07-13

### Fixed

- Fix github repository link in welcome message

## [1.4.2] - 2020-05-09

### Added

- Added an error message when an unhandled message is sent to the bot

## [1.4.1] - 2020-05-08

### Added

- Added github link to the welcome message

## [1.4.0] - 2020-05-08

### Added

- Added in progress messages while the bot is searching for submission to neaten

### Changed

- Neatening up tests

## [1.3.7] - 2020-04-11

### Changed

- Subscription watcher should shutdown more quickly when asked to stop

### Fixed

- Fixing a bug that resulted in repeats when API was down, and instead keeps trying until API returns

## [1.3.6] - 2020-03-22

### Changed

- Updating python-telegram-bot
- Converting tests to pytest

## [1.3.5] - 2020-03-22

### Changed

- Subscription watcher will shutdown faster when asked to stop

### Fixed

- Network issues when getting new results will no longer kill subscription watcher

## [1.3.4] - 2020-03-21

### Fixed

- Writing subscription data to temp file, so that running out of disc space doesn't lead to data loss

## [1.3.3] - 2020-03-21

### Added

- Adding heartbeat pings to subscription watcher, so that downtime can be monitored

## [1.3.2] - 2020-02-09

### Changed

- Moving remaining filters around, merging changes from 1.2.9

## [1.3.1] - 2020-01-13

### Fixed

- Fixing the filter used for image hash recommendation

## [1.3.0] - 2020-01-13

### Added

- Recommending @FindFurryPicBot to find image sources, if they are sent without a link

## [1.2.9] - 2020-02-09

### Changed

- Pulling all functionality classes and filter classes out into their own files

## [1.2.8] - 2019-12-27

### Fixed

- Don't repost submissions on bot restart, save latest submission IDs after every post

## [1.2.7] - 2019-12-08

### Fixed

- Use message queue to avoid messages hitting flood limit and failing to send

## [1.2.6] - 2019-11-30

### Changed

- Load and send submissions one by one, rather than waiting to load them all before sending any to subscriptions

## [1.2.5] - 2019-11-29

### Fixed

- Fixing "list changed size during iteration" error in subscription watcher by copying subscription list before
  iterating it

## [1.2.4] - 2019-11-29

### Fixed

- Bot should now respond to commands with the username appended to them, such as commands sent in groups

## [1.2.3] - 2019-11-29

### Changed

- List subscriptions command should list them in order

## [1.2.2] - 2019-11-29

### Changed

- Speed up subscription watcher by fetching all submissions before checking subscriptions

## [1.2.1] - 2019-11-28

### Changed

- Adding prefix to subscription result posts, rather than having a separate message saying which subscription the
  submission was posted for

### Fixed

- Adding retry logic to the FA API handler, to handle intermittent errors

## [1.2.0] - 2019-11-10

### Added

- Initial version number
- Rest of the bot, technically.

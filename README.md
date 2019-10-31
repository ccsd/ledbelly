# LEDbelly

LEDbelly, or Live Events Daemon is powerful and lightweight middleware for consuming CanvasLMS Live Events, from SQS to SQL.

- provides a fast multi-threaded message processor via [Shoryuken](https://github.com/phstc/shoryuken)
- provides support and compatibility for popular databases used by the Canvas Community via [Sequel](https://github.com/jeremyevans/sequel)
- currently supports MSSQL, MySQL, PostgreSQL, and Oracle[*](https://github.com/ccsd/ledbelly#known-issues)
- supports both [Canvas Raw](https://github.com/instructure/canvas-lms/blob/master/doc/api/live_events.md) and [IMS Caliper](https://github.com/instructure/canvas-lms/blob/master/doc/api/caliper_live_events.md) formats

Creating an SQS Queue and receiving events is easy and relatively low cost via [AWS](https://aws.amazon.com/sqs/pricing/), but consuming and using events is a bit more complicated. Live Events are published in real-time as users or the system performs actions within Canvas. This real-time data is extremely useful for building applications and services. Event messages are published to Amazon Simple Queue Service (SQS) in JSON. Some of the data is nested in the Canvas format, and all events are nested in the Caliper format. This creates a problem for pushing events to SQL since data isn't structured for this purpose. There are many options for software and AWS services like Glue can do this easily and efficiently, for a price. However, with a little configuration and a dedicated server, LEDbelly will help you add Live Events into your existing Canvas Data SQL pipeline.

LEDbelly aims to make using Live Events Services easy and attainable to any school or institution that cannot afford more robust options and licensing costs. The products that can do this often have costs or overhead out of reach for small budgets and teams. Therefore, LED has been designed to be easily integrated and maintainable for your purposes. Additional features have been implemented to aid in the continual maintenance as Canvas periodically adds new events and new columns to existing events, and more improvements are coming!

I welcome any community contributions and collaboration on this project. I will maintain and update as necessary.

## Features

__Shoryuken__, provides an easy to use multi-threaded SQS processor. Handling events from SQS is [pretty easy](https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/sqs-examples.html), but the number of messages that can come through can go from trickle to firehose instantly when there's a lot of activity in Canvas. Without multi-threading, messages will backup and your real-time data eventually becomes 'when it arrives', likely to catch up in the middle of the night when things slow down.

__Sequel__, provides the support for multiple databases without extra code management. It also provides DB connection pooling to handle the numerous threads inserting rows.

_These two gems were chosen for their active support and contributions by multiple contributors. This allows _us_ to focus on Live Events, and not multi-threaded SQS processing or supporting multiple database adapters and engines._

__Live Events Worker__, is the Shoryuken process that polls messages an passes them to the parser, then handles importing data to SQL.

__Parsers__, are provided for each event format in `src/events`. These files are the product of patiently collecting auto-generated JSON to SQL tables and columns. Then auto generating the code to simplified `CASE` statements for each event type to produce `key value` pairs of _known_ event information. Each event is passed to the parser, and the known fields are defined and datatype are set. 

_Most maintenance happens here, when Canvas adds new columns or events, you'll need to update the code... Hopefully we'll do this collectively and share what we see. It's entirely possible to never see certain events based on how your users use Canvas. So that first school to see new data could write the update and submit a PR so others can benefit._

__Import__ happens after each event is parsed. A `Hash` with non-null fields is passed to the import method. Each string field is passed through its schema definition and trimmed to the defined length, ensuring the row is inserted.

__Schemas__, are configured in `lib/schemas`, and help prepare data before import, and simplify the maintenance cycle. Each schema file for Canvas or Caliper (or custom tables) maintains the event type table definition for each column. This helps LEDbelly know when new fields are added and existing fields are out-of-bounds. The schemas are also used to auto generate the DDL for your preferred database using `rake create_tables` task.

[__Automation Tasks__](https://github.com/ccsd/ledbelly/wiki/Tasks)

[__Logging__](https://github.com/ccsd/ledbelly/wiki/Logging)

__Live Stream__, while LEDbelly will process both Canvas and Caliper formats into their specific event tables, sometimes it's easier to deal with things collectively. Live Stream available in `src/extentions` passes common fields for all Canvas events into a `live_stream` table. This is useful for tracking active users and types of activity without overly cumbersome views and joins. I __recommend__ using this feature, it's available by default, but you can remove it. It's also packaged to give an example of providing your own extensions in case you want to add or manipulate some stream without modifying the defaults. A couple of quick SQL queries in `sql/samples` are provided.


## Getting Started
[Installation & Setup](https://github.com/ccsd/ledbelly/wiki/Getting-Started)


## License
LEDbelly, is distributed under the MIT License

>Copyright (c) 2019 Clark County School District, created by Robert Carroll
>
>Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
>
>The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
>
>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


## Support & Contributions

[I'm available on the CanvasLMS Community](https://community.canvaslms.com/people/carroll-ccsd)

This project it is intended for community use and contributions. My hope is for there to be community collaboration and governance for future development and changes. Canvas cloud hosted instances are updated regularly and changes to Live Events often occur with these updates. Users of LEDbelly, would benefit from working together to continuously maintain this repo.

I am not a Rubyist, and this is my first multi file Ruby project, if you can _school me_ and help improve the code please do!

New code should pass the [RuboCop](https://github.com/rubocop-hq/rubocop) rules supplied. However, one(1) existing Cop fails, for introducing globals, if you know how to work around this issue please share. :D

> For now, simply fork the repo, create a feature branch, commit, and push your changes, and then create a pull request.
> 
> Review and consider [Issues](https://github.com/ccsd/ledbelly/issues)

## Resources
#### Community
- [LEDbelly discussion](https://community.canvaslms.com/message/157736-ledbelly-live-events-daemon-for-canvas-lms-sqs-to-sql)
- [Canvas Live Events | Awesome CanvasLMS](https://community.canvaslms.com/docs/DOC-17354-awesome-canvaslms#CanvasLiveEvents)

#### Canvas Source Files

Watch commits to this file, they usually indicate when you'll see new events or columns.
- [canvas/live_events.rb](https://github.com/instructure/canvas-lms/blob/master/lib/canvas/live_events.rb)
- [canvas/live_events_spec.rb](https://github.com/instructure/canvas-lms/blob/master/spec/lib/canvas/live_events_spec.rb)
- Changes to Live Events are also posted to Canvas Release Notes


## Known Issues

1) Much of the Caliper data where you'd want to have integer ID's contains strings with ID's. I am not currently using Caliper events, but I receive them for development purposes. I have refrained from making decisions for others here, but would expect the desirable choice would be RegEx matching and storing the ID.
```
actor_id: 'urn:instructure:canvas:user:100000000000001'
membership_id: 'urn:instructure:canvas:course:100000000000012:Instructor:100000000000001'
membership_member_id: 'urn:instructure:canvas:user:100000000000001'
membership_member_type: 'Person'
membership_organization_id: 'urn:instructure:canvas:course:100000000000012'
membership_organization_type: 'CourseOffering'
```

2) Oracle's 30 character limit poses problems for Live Events as many of the fields (especially from IMS Caliper) are formatted using underscore_notation. Some guidance from Canvas Community Oracle users would be much appreciated here. While Sequel will successfully handle SQS to SQL for Oracle manually shortening each field that is >30, it would be nice if we could create some uniform shortening for LEDbelly that keeps it simple for everyone.

3) A second Oracle issue exists in the `rake create_tables` task, in order to generate DDL's for each database I wrote a custom task that parses the `lib/schemas/*` from `Hash` to DDL statements. While my dev environment is currently at 12c, the compatibility is at 11, making it hard for me to test a DDL with the simple `CREATE TABLE with IDENTITY` for the PK.

4) `interaction_data` for `quizzes.item_created` and `quizzes.item_updated` needs to be improved, but I am not currently using these columns.

## Credits
Many thanks to Pablo Cantero and Jeremy Evans for their open source contributions. LEDbelly would be much harder to maintain without these Gems.

The name _LEDbelly_ was chosen one morning while listening to [Huddie Ledbetter aka Lead Belly](https://en.wikipedia.org/wiki/Lead_Belly). It seemed apropos for a _Live Events Daemon and Consumer_

[The CCSD Canvas Team](http://obl.ccsd.net)

![Elvis Panda](https://s3-us-west-2.amazonaws.com/ccsd-canvas/branding/images/ccsd-elvis-panda-sm.png "Elvis Panda")
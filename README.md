# LEDbelly

LEDbelly, or Live Events Daemon is powerful and lightweight middleware for consuming CanvasLMS Live Events, from SQS to SQL.

- provides a fast multi-threaded message processor via [Shoryuken](https://github.com/phstc/shoryuken)
- provides support and compatibility for popular databases used by the Canvas Community via [Sequel](https://github.com/jeremyevans/sequel)
- currently supports MSSQL, MySQL, PostgreSQL, and Oracle[*](https://github.com/ccsd/ledbelly#known-issues)
- Supports both [Canvas Raw](https://github.com/instructure/canvas-lms/blob/master/doc/api/live_events.md) and [IMS Caliper](https://github.com/instructure/canvas-lms/blob/master/doc/api/caliper_live_events.md) formats

Creating an SQS Queue and receiving events is easy and relatively low cost via [AWS](https://aws.amazon.com/sqs/pricing/), but consuming and using events is a bit more complicated. Live Events are published in real-time as users or the system performs actions within Canvas. This real-time data is extremely useful for building applications and services. Event messages are published to Amazon Simple Queue Service (SQS) in JSON. Some of the data is nested in the Canvas format, and all events are nested in the Caliper format. This creates a problem for pushing events to SQL since data isn't structured for this purpose. There are many options for software and AWS services like Glue can do this easily and efficiently, for a price. However, with a little configuration and a dedicated server, LEDbelly will help you add Live Events into your existing Canvas Data pipeline.

LEDbelly aims to make using Live Events Services easy and attainable to any school or institution that cannot afford more robust options and licensing costs. The products that can do this often have costs or overhead out of reach for small budgets and teams. So, it has been designed to be easily integrated and maintainable for your purposes. Additional features have been implemented to aid in the continual maintenance of the program as Canvas periodically adds new events and new columns to existing events, and more improvements are coming!

I welcome any community contributions and collaboration on this project. I will maintain and update as necessary.

## Features

__Shoryuken__, provides an easy to use multi-threaded SQS processor. Handling events from SQS is [pretty easy](https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/sqs-examples.html), but the number of messages that can come through can go from trickle to firehose instantly when there's a lot of activity in Canvas. Without multi-threading, messages will backup and your real-time data eventually becomes 'when it arrives', likely to catch up in the middle of the night when things slow down.

__Sequel__, provides the support for multiple databases without extra code management. It also provides DB connection pooling to handle the numerous threads inserting rows.

_These two gems were chosen for their active support and contributions by multiple contributors. This allows _us_ to focus on Live Events, and not multi-threaded SQS processing or supporting multiple database adapters and engines._

__Live Events Worker__, is the Shoryuken process that polls messages an passes them to the parser, then handles importing data to SQL.

__Parsers__, are provided for each event format in `src/events`. These files are the product of patiently collecting auto-generated JSON to SQL tables and columns. Then auto generating the code to simplified `CASE` statements for each event type to produce `key value` pairs of _known_ event information. Each event is passed to the parser, and the known fields are defined and datatype are set. 

_Most maintenance happens here, when Canvas adds new columns or events, you'll need to update the code... Hopefully we'll do this collectively and share what we see. It's entirely possible to never see certain events based on how your users use Canvas. So that first school to see new data should update the code and submit a PR so others can update._

__Import__ happens after each event is parsed. A `Hash` with non-null fields is passed to the import method. Each string field is passed through its Schema definition and trimmed to the defined length, ensuring the row is inserted.

__Schemas__, are configured in `lib/schemas`, and help prepare data before import, and simplify the maintenance cycle. Each schema file for Canvas or Caliper (or custom tables) maintains the event type table definition for each column. This helps LEDbelly know when new fields are added and existing fields are out-of-bounds. The schemas are also used to auto generate the DDL for your preferred database using `rake create_tables` task.

__Create Tables__, or `rake create_tables` will use the database.yml `adapter` to generate the DDL files for each schema. An attempt was made to use Sequel's existing features to handle this, but problems arise when trying to handle multi-byte character strings for multiple databases. So the Rake task is a current _best effort_ attempt to handle this for the 4 supported databases.

__Logging__, various log points are provided to catch the following:
- `String` value before insert is longer than defined. The string will be truncated to the defined length (accounting to multi-byte strings/storage), and the `log/sql-truncations.log` file will collect these. Use this to update the `lib/schemas` files.
- SQL errors will land in `log/sql-errors.log`, these are typically simple issues currently unaddressed by the code. The log will contain the event name, the error, the SQL statement that failed, and the Hash provided for insert.
- Body Ccount, for Canvas Raw events, and soon for Caliper. If an event was received with more columns that we have defined, the log will contain the list of new fields we need to add to the code and schema.
- SQL recovery log `logs/sql-recovery.sql` will contain any failed SQL statement. This is useful if you want to recover the lost data easily after updating.
- [Logger](https://github.com/ruby/logger), can be enabled in `ledbelly.rb` for the database connection. This will log every transaction by day. The files will become huge. It's disabled by default and provided for debugging purposes.
- Shoryuken, has it's own logging options, they are covered below. I only run shoryuken logging for debugging purposes.

__Live Stream__, while LEDbelly will process both Canvas and Caliper formats into their specific event tables, sometimes it's easier to deal with things collectively. Live Stream available in `src/extentions` passes common fields for all Canvas events into a `live_stream` table. This is useful for tracking active users and types of activity without overly cumbersome views and joins. I __recommend__ keeping this table and process in play, it's available by default, but you can remove it. It's also packaged to give an example of providing your own extensions in case you want to add or manipulate some stream without modifying the defaults. A couple of quick SQL queries in `sql/samples` are provided.


## Getting Started

Tested with Ruby 2.5.1, but I'm currently running Ruby 2.6.1 and [Bundler](https://bundler.io/)

1) Edit `cfg/sqs.yml.example` and save as `sqs.yml`
2) Edit `cfg/database.yml.example` and save as `database.yml`
3) Edit `Gemfile`, choosing (uncomment) the appropriate driver adapter for your database.
	- `gem tiny_tds` for MS SQL Server*
	- `gem mysql2` for MySQL
	- `gem pg` for PostgreSQL*
	- `gem ruby-oci8` for Oracle


	For SQL Server, you may need to install and configure freetds first.
	
	If PostgreSQL is installed on another system, you will need to install libpq first and then install the Ruby GEM 'pg' with options.
    `gem install pg -v '1.0.0' -- --with-opt-dir="/usr/local/opt/libpq"`
4) Run `bundle install`
5) Run `rake create_tables`, evaluate the schema files and run them against your db instance
	
    - You may choose to use only the Canvas Raw or the Caliper Formats for your database. LEDBelly is available to process either all the time.
	- Add schemas and manage tables for any `extensions` you use or create like __Live Stream__
6) Start LEDbelly, from the directory root

    `bundle exec shoryuken -r ./ledbelly -C cfg/sqs.yml -L /dev/null -d`
    
    -d flag daemonizes Shoryuken and detaches the terminal
    Note, you will get an error if you try to daemonize Shoryuken without logging, so `/dev/null` works.
    
    __[FAIL] You should set a logfile if you're going to daemonize__
7) Daemonize the Daemon

	Once LEDbelly is installed and working you may want to run it as a service. So that, if it crashes, or disconnects it will retry or restart when the system restarts. There are probably as many ways to do this as operating systems, but I've provided an example of what I'm trying on RHEL in [SYSTEMD.md](SYSTEMD.md)

### Other startup options

terminal output __without__ _Shoryuken_ logging

`bundle exec shoryuken -r ./ledbelly -C cfg/sqs.yml`

terminal output __with__ _Shoryuken_ logging

`bundle exec shoryuken -r ./ledbelly -C cfg/sqs.yml -L log/shoryuken.log`


## License
LEDbelly, is distributed under the  MIT License

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

[You may contact me via the CanvasLMS Community](https://community.canvaslms.com/people/carroll-ccsd)

While there is currently only 1 stakeholder for this project it is intended for community use and contributions. As such, there will need to be some community collaboration and governance for future development and changes. Canvas cloud hosted instances are updated every 3 weeks, changes to Live Events often occur with these updates. Although, I typically see most changes (new events and columns) added Tuesday night. Users of LEDbelly, would benefit from working together to continuously maintain this repo.

I am not a Rubyist, and this is my first multi file Ruby project, if you can _school me_ and help improve the code please do!

New code should pass the [RuboCop](https://github.com/rubocop-hq/rubocop) rules supplied. However, one(1) existing Cop fails, for introducing globals, if you know how to work around this issue please share. :D

>For now, simply fork the repo, create a feature branch, commit, and push your changes, and then create a pull request.


## Resources

Community:
- [Live Events Services - Table of Contents](https://community.canvaslms.com/docs/DOC-15740-live-events-services-table-of-contents)
- [SQS Queue setup for Live Events](https://community.canvaslms.com/docs/DOC-14163-how-do-i-create-an-sqs-queue-to-receive-live-events-data-from-canvas)

Watch commits to this file, they usually indicate when you'll see new events or columns.
- [Canvas Live Events Github](https://github.com/instructure/canvas-lms/blob/master/lib/canvas/live_events.rb)

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
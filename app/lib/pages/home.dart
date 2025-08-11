import 'dart:developer';

import 'package:app/graphql/command.dart';
import 'package:flutter/material.dart';
import 'package:graphql/client.dart';

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key});

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  String ledStatus = '';

  @override
  void initState() {
    // TODO: implement initState
    super.initState();
    subscribeGraphQL();
  }

  subscribeGraphQL() {
    client.subscribe(SubscriptionOptions(document: gql(ledSub))).listen((res) {
      setState(() {
        ledStatus = res.data!['subscribeTopicMessages']['content'];
        log('content:$ledStatus');
      });
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('GraphQL + MQTT')),
      body: Column(
        children: [
          // status
          Text(ledStatus),

          // button on
          ElevatedButton(onPressed: () => sendOn(), child: Text('On')),

          // button off
          ElevatedButton(onPressed: () => sendOff(), child: Text('Off')),
        ],
      ),
    );
  }

  Future<void> sendOn() async {
    final MutationOptions options = MutationOptions(document: gql(ledON));
    final QueryResult result = await client.mutate(options);

    if (result.hasException) {
      log(result.exception.toString());
      return;
    }

    log('content:${result.data!['sendToTopic']['content']}');

    setState(() {
      ledStatus = result.data!['sendToTopic']['content'];
    });
  }

  Future<void> sendOff() async {
    final MutationOptions options = MutationOptions(document: gql(ledOFF));
    final QueryResult result = await client.mutate(options);

    if (result.hasException) {
      log(result.exception.toString());
      return;
    }

    log('content:${result.data!['sendToTopic']['content']}');

    setState(() {
      ledStatus = result.data!['sendToTopic']['content'];
    });
  }
}

// TODO: should send variable to query
import 'package:graphql/client.dart';

const ledOFF = """
mutation {
  sendToTopic(input: {
    topic: "test/topic"
    content: "LED_OFF"
    sender: "user1"
  }) {
    id
    topic
    content
    timestamp
  }
}
""";

const ledON = """
mutation {
  sendToTopic(input: {
    topic: "test/topic"
    content: "LED_ON"
    sender: "user1"
  }) {
    id
    topic
    content
    timestamp
  }
}
""";

const ledSub = """
subscription {
  subscribeTopicMessages(topic: "test/topic") {
    id
    content
    timestamp
  }
}
""";

const httpEndpoint = 'http://10.0.2.2:8000/graphql';
const websocketEndpoint = 'ws://10.0.2.2:8000/graphql';

late GraphQLClient client;

gqlConnect() {
  final _httpLink = HttpLink(httpEndpoint);

  Link _link = _httpLink;

  /// subscriptions must be split otherwise `HttpLink` will. swallow them
  final _wsLink = WebSocketLink(websocketEndpoint);
  _link = Link.split((request) => request.isSubscription, _wsLink, _httpLink);

  client = GraphQLClient(
    /// **NOTE** The default store is the InMemoryStore, which does NOT persist to disk
    cache: GraphQLCache(),
    link: _link,
  );
}

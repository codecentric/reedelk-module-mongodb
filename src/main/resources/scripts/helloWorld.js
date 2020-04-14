function helloWorld(context,message) {

    var attributes = message.attributes();
    var pathParams = attributes.pathParams;

    var result = 'Hello World';
    if (pathParams.containsKey('name')) {
        result += ' ' + pathParams.get('name');
    }

    return result;
}
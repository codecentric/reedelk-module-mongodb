function test(context, message) {
    var stringified = JSON.stringify(message.payload());
    Log.info(message.payload());
	var text = 'test function';
	return text + ' result';
}
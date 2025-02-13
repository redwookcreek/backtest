var ticker = WScript.arguments(0);
var start_date = WScript.arguments(1);
var end_date = WScript.arguments(2);

AB = new ActiveXObject("Broker.Application");
AB.ActiveDocument.Name = ticker;
AB.ActiveWindow.ZoomToRange(start_date, end_date);

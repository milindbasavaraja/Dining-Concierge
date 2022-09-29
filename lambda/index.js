exports.handler = async (event) => {
  // TODO implement
  var d = new Date();
  var m = d.getMinutes();
  var h = d.getHours();
  var time = `${h}:${m}`;
  const response = {
    // statusCode: 200,

    messages: [
      {
        type: "unstructured",
        unstructured: {
          id: "1",
          text: "Application Under development.Search functionality will be implemented in Assignment-2",
          timestamp: time,
        },
      },
    ],
    body: "Application Under development.Search functionality will be implemented in Assignment-2",
  };
  return response;
};

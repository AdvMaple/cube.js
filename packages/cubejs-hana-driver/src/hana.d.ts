declare module '@sap/hana-client/extension/TypeCode' {
  const TypeCode: Record<string, unknown>;
  export = TypeCode;
}

declare module '@sap/hana-client/extension/Stream' {
  const Stream: {
    createObjectStream(rs: unknown): NodeJS.ReadableStream;
  };
  export = Stream;
}

module {
  public type Timestamp = Int;

  public type ApiResult<T> = {
    #ok : T;
    #err : Text;
  };
};

package test;

public class Bean {
    private Long id;
    private Long uid;
    private String address;
    private String uaddress;
    private Long time;

    public Bean(Long id,Long uid,String address,String uaddress,Long time){
        this.id = id;
        this.uid = uid;
        this.address = address;
        this.uaddress = uaddress;
        this.time = time;

    }

    @Override
    public String toString() {
        return "Bean{" +
                "id=" + id +
                ", uid=" + uid +
                ", address='" + address + '\'' +
                ", uaddress='" + uaddress + '\'' +
                ", time=" + time +
                '}';
    }
}

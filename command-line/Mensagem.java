package sistemakv;

import com.google.gson.Gson;

public class Mensagem {
    
    public static final int PUT = 1;
    public static final int PUT_OK = 2;
    public static final int GET = 3;
    public static final int REPLICATION = 4;
    public static final int REPLICATION_OK = 5;
    public static final int TRY_OTHER_SERVER_OR_LATER = 6;
    
    public static final int MODE_SEND = 1;
    public static final int MODE_RESPONSE = 2;
    public static final int MODE_REDIRECT = 3;
    public static final int MODE_SEND_LISTEN = 4;
    
    private int modo;
    private int tipo;
    private String chave;
    private String valor;
    private String ipPortaOrigem;
    private String ipPortaDestino;
    private String timestamp;
    
    public Mensagem() {
    }
    
    public static String serializar(Mensagem mensagem) {
        Gson gson = new Gson();
        String json = gson.toJson(mensagem);
        
        return json;
    }
    
    public static Mensagem desserializar(String json) {
        Gson gson = new Gson();
        Mensagem mensagem = gson.fromJson(json, Mensagem.class);
        return mensagem;
    }
    
    public static Mensagem criarPutClient(String ipPortaOrigem, String ipPortaDestino, String chave, String valor) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setTipo(Mensagem.PUT);
        mensagem.setModo(Mensagem.MODE_SEND_LISTEN);
        mensagem.setIpPortaOrigem(ipPortaOrigem);
        mensagem.setIpPortaDestino(ipPortaDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        
        return mensagem;
    }
    
    public static Mensagem criarPut(String ipPortaOrigem, String ipPortaDestino, String chave, String valor, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_REDIRECT);
        mensagem.setTipo(Mensagem.PUT);
        mensagem.setIpPortaOrigem(ipPortaOrigem);
        mensagem.setIpPortaDestino(ipPortaDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public static Mensagem criarPutOk(String ipPortaOrigem, String ipPortaDestino, String chave, String valor, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_REDIRECT);
        mensagem.setTipo(Mensagem.PUT_OK);
        mensagem.setIpPortaOrigem(ipPortaOrigem);
        mensagem.setIpPortaDestino(ipPortaDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public static Mensagem criarReplication(
            String ipOrigem,
            String ipDestino,
            String chave,
            String valor,
            String timestamp
    ) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(MODE_SEND);
        mensagem.setTipo(Mensagem.REPLICATION);
        mensagem.setIpPortaOrigem((ipOrigem));
        mensagem.setIpPortaDestino(ipDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public static Mensagem criarReplicationOk(String ipOrigem, String chave) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(MODE_RESPONSE);
        mensagem.setTipo(Mensagem.REPLICATION_OK);
        mensagem.setIpPortaOrigem(ipOrigem);
        mensagem.setChave(chave);
        
        return mensagem;
    }
    
    public static Mensagem criarGetClient(String ipOrigem, String ipDestino, String chave, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setTipo(Mensagem.GET);
        mensagem.setModo(Mensagem.MODE_SEND);
        mensagem.setIpPortaOrigem(ipOrigem);
        mensagem.setIpPortaDestino(ipDestino);
        mensagem.setChave(chave);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public static Mensagem criarGet(String ipOrigem, String chave, String valor, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_RESPONSE);
        mensagem.setTipo(Mensagem.GET);
        mensagem.setIpPortaOrigem(ipOrigem);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public static Mensagem criarRetry(String ipOrigem, String chave, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_RESPONSE);
        mensagem.setTipo(Mensagem.TRY_OTHER_SERVER_OR_LATER);
        mensagem.setIpPortaOrigem(ipOrigem);
        mensagem.setChave(chave);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public int getModo() {
        return this.modo;
    }
    
    public void setModo(int modo) {
        this.modo = modo;
    }
    
    public int getTipo() {
        return this.tipo;
    }
    
    public void setTipo(int tipo) {
        this.tipo = tipo;
    }
    
    public String getChave() {
        return this.chave;
    }
    
    public void setChave(String chave) {
        this.chave = chave;
    }
    
    public String getValor() {
        return this.valor;
    }
    
    public void setValor(String valor) {
        this.valor = valor;
    }
    
    public String getTimestamp() {
        return this.timestamp;
    }
    
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getIpPortaOrigem() {
        return this.ipPortaOrigem;
    }
    
    public void setIpPortaOrigem(String ipPortaOrigem) {
        this.ipPortaOrigem = ipPortaOrigem;
    }
    
    public String getIpPortaDestino() {
        return this.ipPortaDestino;
    }
    
    public void setIpPortaDestino(String ipPorta) {
        this.ipPortaDestino = ipPorta;
    }
}

package model;

import java.math.BigDecimal;

public class Venda {
    private Long operacao;
    private Long cliente;
    private int quantidadeIngressos;
    private BigDecimal valorTotal;
    private String status;

    public Long getOperacao() {
        return operacao;
    }

    public void setOperacao(Long operacao) {
        this.operacao = operacao;
    }

    public Long getCliente() {
        return cliente;
    }

    public void setCliente(Long cliente) {
        this.cliente = cliente;
    }

    public int getQuantidadeIngressos() {
        return quantidadeIngressos;
    }

    public void setQuantidadeIngressos(int quantidadeIngressos) {
        this.quantidadeIngressos = quantidadeIngressos;
    }

    public BigDecimal getValorTotal() {
        return valorTotal;
    }

    public void setValorTotal(BigDecimal valorTotal) {
        this.valorTotal = valorTotal;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Venda{" +
                "operacao=" + operacao +
                ", cliente=" + cliente +
                ", quantidadeIngressos=" + quantidadeIngressos +
                ", valorTotal=" + valorTotal +
                ", status='" + status + '\'' +
                '}';
    }

    public Venda(Long operacao, Long cliente, int quantidadeIngressos, BigDecimal valorTotal) {
        this.operacao = operacao;
        this.cliente = cliente;
        this.quantidadeIngressos = quantidadeIngressos;
        this.valorTotal = valorTotal;
    }

    public Venda() {
    }
}

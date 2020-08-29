/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package javahbase;

import java.io.Serializable;

/**
 *
 * @author nouran
 */
public class AggregateData implements Serializable{
    public static final long serialVersionID=1L;

    public Long getOrderbelow5000() {
        return orderbelow5000;
    }

    public void setOrderbelow5000(Long orderbelow5000) {
        this.orderbelow5000 = orderbelow5000;
    }

    public Long getOrderbelow10000() {
        return orderbelow10000;
    }

    public void setOrderbelow10000(Long orderbelow10000) {
        this.orderbelow10000 = orderbelow10000;
    }

    public Long getOrderbelow20000() {
        return orderbelow20000;
    }

    public void setOrderbelow20000(Long orderbelow20000) {
        this.orderbelow20000 = orderbelow20000;
    }

    public Long getOrderabove20000() {
        return orderabove20000;
    }

    public void setOrderabove20000(Long orderabove20000) {
        this.orderabove20000 = orderabove20000;
    }

    public Long getTotalOrder() {
        return TotalOrder;
    }

    public void setTotalOrder(Long TotalOrder) {
        this.TotalOrder = TotalOrder;
    }
    private Long orderbelow5000=0l;
    private Long orderbelow10000=0l;
    private Long orderbelow20000=0l;
    private Long orderabove20000=0l;
    private Long TotalOrder=0l;

    
}

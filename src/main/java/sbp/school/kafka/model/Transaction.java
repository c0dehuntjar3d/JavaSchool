package sbp.school.kafka.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    @Getter
    public enum TransactionType {
        TRANSFER("Перевод"),
        CREDIT("Кредитование"),
        BROCKER_SERVICE("Брокерские услуги"),
        CASH_TRANSITION("Кассовые операции"),
        ;

        private TransactionType(String name) {
            this.name = name;
        }

        private String name;
    }

    private String id;
    private TransactionType type;
    private BigDecimal value;
    private String account;
    private LocalDateTime date;

}

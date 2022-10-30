package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"strconv"
	"time"
	// 	"fmt"
)

type server struct {
	db *sql.DB           // server -   структура для обращения к базе данных sql
}

type user struct {     //    user - тип данных структура 
	UserId int           //  - идентификатор пользователя целое
	Amount int           //  -  счет пользователя  целое
}

type accountKeeper struct {  //   accountKeeper  - структура 
	Users []user               //  Users  - срез с типом данных user     
}

type historyEntry struct {     // historyEntry      - структура для фиксации транзакций
	UserId  int                  //   - идентификатор пользователя целое
	IsDebit bool                 //   - флаг совершения операции
	Amount    int                //    -  сумма операции
	TransTime time.Time          //   - время совершения операции
}

type history struct {           //  - журнал совершенных операций
	Histories []historyEntry      //  - Histories срез с типом данных historyEntry
}

//  responseError   - функция для логирования ошибок в json формате

func responseError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"Error": message})
}

//  responseJSON  - функция для записи ответов в формат json на сервер

func responseJSON(w http.ResponseWriter, response []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// getBalance  - функция возвращает баланс пользователя, либо ошибку

func (s *server) getBalance(w http.ResponseWriter, r *http.Request) {
	queryString := r.URL.Query()
	userId, err := strconv.Atoi(queryString.Get("user_id"))  //  обращаемся по user_id к БД 
	if err != nil {                                          // при отсутствии user_id в БД - вызов  ф-ции  responseError и запись ошибки
		responseError(w,"there is no user_id in request", http.StatusBadRequest)
		return
	}
	var _userId, _amount int                     //  формируем SQL запрос к БД, ищем поля "user_id", "amount" в таблице "users" по значению userId
	err = s.db.QueryRow(`SELECT "user_id", "amount" FROM "users" where "user_id"=$1`, userId).Scan(&_userId, &_amount)
	switch {                                    //  обработка возможных ошибок
	case err == sql.ErrNoRows:
		responseError(w, "there is no rows with given user_id", http.StatusNotFound)
		return
	case err != nil:
		responseError(w, "some problems in db query", http.StatusInternalServerError)
		return
	}
	usersRes := accountKeeper{Users: []user{user{UserId: userId, Amount: _amount}}}          // получаем результат остатка пользователя из структуры accountKeeper
	response, err := json.Marshal(usersRes)                                                  // упаковываем  в json (маршалим)
	if err != nil {
		responseError(w,"err in result marshaling", http.StatusInternalServerError)           // ошибка неудачи при маршалинге    
		return
	}
	responseJSON(w, response)                                                              // запись ответа на сервер (вызовом ф-ции responseJSON)
}

//   getUserHistory  отчет об операциях пользователя

func (s *server) getUserHistory(w http.ResponseWriter, r *http.Request) {
	queryString := r.URL.Query()
	userId, err1 := strconv.Atoi(queryString.Get("user_id"))
	lastNOperations, err2 := strconv.Atoi(queryString.Get("n_last_operations"))
	if err1 != nil || err2 != nil {                           //  проверка на наличие ошибок при обращении к БД по user_id и n_last_operations
		responseError(w, "there is no user_id in request or n_last_operations", http.StatusBadRequest)
		return
	}
	var _userId, _amount int
	var _isDebit bool
	var _time time.Time
	historyRes := history{}
	historyRes.Histories = make([]historyEntry, 0)       //  формируем SQL запрос к БД, ищем  user_id, is_debit, amount, time в таблице "history" по userId и  lastNOperations
	row, err := s.db.Query(`SELECT user_id, is_debit, amount, time FROM "history" where "user_id"=$1 ORDER BY id DESC LIMIT $2`,
		userId, lastNOperations)
	if err != nil {                    //  запись ошибки
		responseError(w, "some problems in request to db", http.StatusInternalServerError)
		return
	}
	defer row.Close()      //  закрываем обращение к таблице, при завершении функции
	for row.Next() {       //  цикл по строкам таблицы
		err := row.Scan(&_userId, &_isDebit, &_amount, &_time)
		if err != nil {      // при ошибке логируем и выходим из ф-ции
			responseError(w, "some problems in scanning row", http.StatusNotFound)
			return
		}
		_tmpHist := historyEntry{_userId, _isDebit, _amount, _time}
		historyRes.Histories = append(historyRes.Histories, _tmpHist)   // добавляем в срез найденнные значения
	}
	response, err := json.Marshal(historyRes)                         // маршалим ответ
	if err != nil {
		responseError(w,"err in result marshaling", http.StatusInternalServerError)
		return
	}
	responseJSON(w, response)                                       //  результат ф-ции
}

// isUserCreated проверка существования пользователя в БД, возращает true/false и ошибку

func (s *server) isUserCreated(userId int) (bool, error) {
	var u, a int
	err := s.db.QueryRow(`SELECT "user_id", "amount" FROM "users" where "user_id"=$1`, userId).Scan(&u, &a)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errors.New("DB_CONN_ERR")
	}
	return true, nil
}

//  topUpBalance  - зачисление средств на счет пользователя

func (s *server) topUpBalance(w http.ResponseWriter, r *http.Request) {
	queryString := r.URL.Query()
	userId, err1 := strconv.Atoi(queryString.Get("user_id"))                 //    запрос на user_id
	accruedAmount, err2 := strconv.Atoi(queryString.Get("accrued_amount"))   //    запрос на accrued_amount
	if err1 != nil || err2 != nil || accruedAmount <= 0 {        // обработка ошибок на  user_id и accrued_amount , выход
		responseError(w, "there is no user_id or accrued_amount in request", http.StatusBadRequest)
		return
	}
	currentTime := time.Now()
	isUserCreated, err := s.isUserCreated(userId)    //  проверка существования пользователя в таблице БД
	if err != nil {
		responseError(w, "some problems in db query", http.StatusInternalServerError)
		return
	}
	if isUserCreated {   // если пользователь есть, SQL запрос к БД на обновление счета пользователя в таблице "amount"="amount" + accruedAmount
		_, err1 = s.db.Exec(`UPDATE "users" set "amount"="amount" + $1 where "user_id"=$2`, accruedAmount, userId)
		if err1 != nil {                 // обработка ошибки, выход
			responseError(w, "problems in updating result ", http.StatusInternalServerError)
			return
		}
	} else {        //  если пользователя нет в таблице "users" БД, то формируем SQL запрос на добавление БД в таблицу нового user_id, amount
		_, err1 = s.db.Exec(`INSERT INTO "users" (user_id, amount) VALUES ($1, $2)`, userId, accruedAmount)
		if err1 != nil {              // обработка ошибки, выход
			responseError(w, "problems in inserting result ", http.StatusInternalServerError)
			return
		}
	}
	_, err = s.db.Exec(`INSERT INTO "history" (user_id, is_debit, amount, time) VALUES ($1, $2, $3, $4)`,
		userId, false, accruedAmount, currentTime)  // делаем запись в таблицу "history" 
	if err != nil {                 // обработка ошибки, выход
		responseError(w,"problems in inserting history table", http.StatusInternalServerError)
		return
	}
	responseJSON(w, []byte(`{"Response": "ok" }`))    // ответ ф-ции
}

//  writeOffMoney  - порверка проведения платежа

func (s *server) writeOffMoney(w http.ResponseWriter, r *http.Request) {
	queryString := r.URL.Query()
	userId, err1 := strconv.Atoi(queryString.Get("user_id"))
	debitedAmount, err2 := strconv.Atoi(queryString.Get("debited_amount"))       //    сумма к списанию со счета пользователя
	if err1 != nil || err2 != nil || debitedAmount <= 0 {                        // обработка ошибок, выход
		responseError(w, "there is no user_id or debited_amount in request", http.StatusBadRequest)
		return
	}
	currentTime := time.Now()
	ctx := context.Background()             //  ctx и tx не совсем понимаю назначение и что делают context.Background() ,  s.db.BeginTx(ctx, nil) 
	tx, err := s.db.BeginTx(ctx, nil)       //     
	if err != nil {                         // обработка ошибок, выход
		responseError(w, "trans problem", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()                   //  закрываем обращение к  tx , после выхода из ф-ции
	var _userId, _amountUser int          // обращение к БД , с обновлением _amountUser 
	err1 = tx.QueryRowContext(ctx, `SELECT "user_id", "amount" FROM "users" where "user_id"=$1 FOR UPDATE`,
		userId).Scan(&_userId, &_amountUser)
	switch {                                // обработка ошибок, выход
	case err1 == sql.ErrNoRows:             
		responseError(w, "there is no user_id "+strconv.Itoa(userId), http.StatusNotFound)
		return
	case err1 != nil:
		responseError(w, "some problems in request to db", http.StatusInternalServerError)
		return
	}
	if _amountUser < debitedAmount {           //  проверка -  счет меньше суммы списания
		responseError(w, "ABORT: User amount should be greater or equal than debited_amount", http.StatusInternalServerError)
		return
	}
                      // запрос на  обновление в таблице "users" значения "amount"="amount" - debitedAmount , списание средств
	_, err1 = tx.ExecContext(ctx, `UPDATE "users" set "amount"="amount" - $1 where "user_id"=$2`, debitedAmount, userId)
	if err1 != nil {                 // обработка ошибок, выход
		responseError(w, "problems on setting res to db", http.StatusInternalServerError)
		return
	}
                     // запрос на запись в таблицу  "history" операции списания средств со счета пользователя
	_, err = tx.ExecContext(ctx, `INSERT INTO "history" (user_id, is_debit, amount, time) VALUES ($1, $2, $3, $4)`,
		userId, true, debitedAmount, currentTime)
	if err != nil {    // обработка ошибок, выход
		responseError(w, "problems in updating history table", http.StatusInternalServerError)
		return
	}
	if err = tx.Commit(); err != nil {    // проверка на запись в БД
		responseError(w, "transaction problem", http.StatusInternalServerError)
		return
	}
	responseJSON(w, []byte(`{"Response": "ok" }`))     // запись положительного результата выполнения функции
}

//   transferMoney   -  перевод средств 

func (s *server) transferMoney(w http.ResponseWriter, r *http.Request) {
	queryString := r.URL.Query()
	fromUserId, err1 := strconv.Atoi(queryString.Get("from_user_id"))      // пользователь кто переводит
	toUserId, err2 := strconv.Atoi(queryString.Get("to_user_id"))          //  кто получат
	amount, err3 := strconv.Atoi(queryString.Get("amount"))                //  сумма перевода
	if err1 != nil || err2 != nil || err3 != nil || amount <= 0 {          // обработка ошибок, выход
		responseError(w, "there is no user_id or update_amount in request", http.StatusBadRequest)
		return
	}
	currentTime := time.Now()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)          // запись транзакции в БД 
	if err != nil {                           // обработка ошибок, выход
		responseError(w, "trans problem in creating transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()        // закрытие отката
	var _userId1, _userId2, amountUserFrom, _amountUserTo int
                        //       запрос на изменение суммы списания перевода
	err1 = tx.QueryRowContext(ctx, `SELECT "user_id", "amount" FROM "users" where "user_id"=$1 FOR UPDATE`,
		fromUserId).Scan(&_userId1, &amountUserFrom)
	if err1 == sql.ErrNoRows {            // обработка ошибок, выход
		responseError(w, "there is no from_user_id "+strconv.Itoa(fromUserId), http.StatusNotFound)
		return
	}
	if err1 != nil {       // обработка ошибок, выход
		responseError(w, "some problems in request to db", http.StatusInternalServerError)
		return
	}
	if amountUserFrom < amount {        //  проверка на достаточность средств при списании
		responseError(w, "ABORT: Amount at from_user should be greater or equal than update_amount", http.StatusBadRequest)
		return
	}
                          // запрос на зачисление суммы 
	err2 = tx.QueryRowContext(ctx, `SELECT "user_id", "amount" FROM "users" where "user_id"=$1 FOR UPDATE`,
		toUserId).Scan(&_userId2, &_amountUserTo)
	isNotExistedToUserId := false     // флаг 
	if err2 == sql.ErrNoRows {        // при ошибке err2  меняем флаг 
		isNotExistedToUserId = true
	}
	if err2 != nil && err2 != sql.ErrNoRows {     // обработка ошибки, выход
		responseError(w, "some problems in request to db", http.StatusInternalServerError)
		return
	}
                                        	// списываем сумму у пользователя
	_, err1 = tx.ExecContext(ctx, `UPDATE "users" set "amount"="amount" - $1 where "user_id"=$2`, amount, fromUserId)
	if isNotExistedToUserId {               // при флаге == 0 ,  создаем нового пользователя , кому переводим средства     
		_, err2 = tx.ExecContext(ctx, `INSERT INTO "users" (user_id, amount) VALUES ($1, $2)`, toUserId, amount)
	} else {                                // при флаге == 1 ,  обновляем значениеу  пользователя , кому переводим средства 
		_, err2 = tx.ExecContext(ctx, `UPDATE "users" set "amount"="amount" + $1 where "user_id"=$2`, amount, toUserId)
	}
	if err1 != nil || err2 != nil {      // обработка ошибки, выход
		responseError(w, "problems on setting res to db", http.StatusInternalServerError)
		return
	}
                                   //  вставляем операцию списание  в таблицу   "history"  
	_, err = tx.ExecContext(ctx, `INSERT INTO "history" (user_id, is_debit, amount, time) VALUES ($1, $2, $3, $4)`,
		fromUserId, true, amount, currentTime)
	if err != nil {    // обработка ошибки, выход
		responseError(w, "problems in updating history table", http.StatusInternalServerError)
		return
	}
                                  //  вставляем операцию зачисления  в таблицу   "history"
	_, err = tx.ExecContext(ctx, `INSERT INTO "history" (user_id, is_debit, amount, time) VALUES ($1, $2, $3, $4)`,
		toUserId, false, amount, currentTime)
	if err != nil {             // обработка ошибки, выход
		responseError(w, "problems in updating history table", http.StatusInternalServerError)
		return
	}
	if err = tx.Commit(); err != nil {         //   логирование ошибки при tx.Commit()
		responseError(w, "transaction problem", http.StatusInternalServerError)
		return
	}
	responseJSON(w, []byte(`{"Response": "ok" }`))     // запись положительного результата выполнения функции
}

func main() {
	db, err := sql.Open("postgres",        // открываем БД и параметры БД
		"host=postgres port=5432 user=postgres password=postgres dbname=account_keeper sslmode=disable")
	if err != nil {     // обработка ошибки обращения к БД, выход
		log.Fatal(err)
	}
	defer db.Close()     // закрываем БД после завершения  main() 
	s := server{db: db}
	http.HandleFunc("/get_balance", s.getBalance)              //  ручки,  http  запросы к БД
	http.HandleFunc("/get_user_history", s.getUserHistory)
	http.HandleFunc("/top_up_balance", s.topUpBalance)
	http.HandleFunc("/write_off_money", s.writeOffMoney)
	http.HandleFunc("/transfer_money", s.transferMoney)
	log.Println("Starting server on :3000...")
	log.Fatal(http.ListenAndServe(":3000", nil))
	//  go mod init avito_server
	//  export GO111MODULE="on"
	// 	go get -u github.com/lib/pq
}

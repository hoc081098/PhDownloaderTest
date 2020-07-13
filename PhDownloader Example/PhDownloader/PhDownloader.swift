//
//  PhDownloader.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay
import RealmSwift
import RxRealm
import RxAlamofire
import Alamofire

// MARK: - PhDownloader

public protocol PhDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState>

  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String

  var downloadResult$: Observable<PhDownloadResult> { get }

  func enqueue(_ request: PhDownloadRequest) -> Completable

  func cancel(by identifier: String) -> Completable

  func cancelAll() -> Completable
}

public enum PhDownloaderError: Error, CustomDebugStringConvertible {
  case databaseError(Error)
  case downloadError(Error)
  case notFound(identifier: String)

  public var debugDescription: String {
    switch self {
    case .downloadError(let error):
      return "Download failure: \(error)."
    case .databaseError(let error):
      return "Database error: \(error)."
    case .notFound(let identifier):
      return "Not found task with identifier: \(identifier)."
    }
  }
}

public typealias PhDownloadResult = Swift.Result<PhDownloadRequest, PhDownloaderError>

public enum PhDownloaderFactory {
  public static func makeDownloader(with options: PhDownloaderOptions) throws -> PhDownloader {
    let dataSource = PhDownloadRealLocalDataSource(realmInitializer: provideRealm)
    return RealSwiftyDownloader(
      options: options,
      dataSource: dataSource
    )
  }
}

// MARK: - PhDownloadLocalDataSource

protocol PhDownloadLocalDataSource {

  /// Update download state for download task
  func update(id: String, state: PhDownloadState) -> Completable

  /// Insert or update download task
  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) -> Completable

  /// Get `Results` by multiple ids
  func getResults(by ids: Set<String>) throws -> Results<PhDownloadTaskEntity>

  /// Get `Results` by single id
  func getResults(by id: String) throws -> Results<PhDownloadTaskEntity>

  /// Get single task by id
  func get(by id: String) throws -> PhDownloadTaskEntity?
}

extension FileManager {
  static func phDownloaderDirectory() throws -> URL {
    let url = FileManager.default
      .urls(for: .documentDirectory, in: .userDomainMask)
      .first!
      .appendingPathComponent("phDownloader", isDirectory: true)

    if !FileManager.default.fileExists(atPath: url.path) {
      try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    }

    return url
  }
}

func provideRealm() throws -> Realm {
  let fileURL = try FileManager.phDownloaderDirectory().appendingPathComponent("phdownloader_default.realm")

  let configuration = Realm.Configuration(
    fileURL: fileURL,
    // Set the new schema version. This must be greater than the previously used
    // version (if you've never set a schema version before, the version is 0).
    schemaVersion: 1,

    // Set the block which will be called automatically when opening a Realm with
    // a schema version lower than the one set above
    migrationBlock: { migration, oldSchemaVersion in
      // We haven’t migrated anything yet, so oldSchemaVersion == 0
      if (oldSchemaVersion < 1) {
        // Nothing to do!
        // Realm will automatically detect new properties and removed properties
        // And will update the schema on disk automatically
      }
    }
  )

  return try Realm(configuration: configuration)
}

public protocol RealmAdapter {
  func objects<Element: Object>(_ type: Element.Type) -> Results<Element>

  func write<Result>(
    withoutNotifying tokens: [NotificationToken],
    _ block: () throws -> Result
  ) throws -> Result

  func add(_ object: Object, update: Realm.UpdatePolicy)

  func delete<Element: Object>(_ objects: Results<Element>)

  func refresh() -> Bool

  func object<Element: Object, KeyType>(ofType type: Element.Type, forPrimaryKey key: KeyType) -> Element?
}

extension Realm: RealmAdapter { }

class PhDownloadRealLocalDataSource: PhDownloadLocalDataSource {

  /// Since we use `Realm` in background thread
  private let realmInitializer: () throws -> RealmAdapter

  /// DispatchQueue that is used to dispatch work items that updated realm `Object`
  private let queue = DispatchQueue(label: "\(PhDownloadRealLocalDataSource.self)", qos: .userInitiated)

  init(realmInitializer: @escaping () throws -> RealmAdapter) {
    self.realmInitializer = realmInitializer
  }

  func update(id: String, state: PhDownloadState) -> Completable {
      .create { obsever -> Disposable in
        let disposable = BooleanDisposable()

        self.queue.async {
          autoreleasepool {
            do {
              if disposable.isDisposed { return }

              let realm = try self.realmInitializer()
              _ = realm.refresh()

              guard let task = Self.find(by: id, in: realm) else {
                return
              }

              if disposable.isDisposed { return }

              try realm.write(withoutNotifying: []) {
                task.state = state.toInt
                task.updatedAt = .init()
              }

              obsever(.completed)
            } catch {
              obsever(.error(error))
            }
          }
        }


        return disposable
    }
  }

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) -> Completable {
      .create { observer -> Disposable in
        let disposable = BooleanDisposable()

        self.queue.async {
          autoreleasepool {
            do {
              if disposable.isDisposed { return }

              let realm = try self.realmInitializer()
              _ = realm.refresh()

              if disposable.isDisposed { return }

              try realm.write(withoutNotifying: []) {
                realm.add(
                  PhDownloadTaskEntity(
                    identifier: identifier,
                    url: url,
                    fileName: fileName,
                    savedDir: savedDir,
                    state: state.toInt
                  ),
                  update: .modified
                )
              }

              observer(.completed)
            } catch {
              observer(.error(error))
            }
          }
        }

        return disposable
    }
  }

  func getResults(by ids: Set<String>)throws -> Results<PhDownloadTaskEntity> {
    try self.realmInitializer()
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier IN %@", ids)
  }

  func getResults(by id: String)throws -> Results<PhDownloadTaskEntity> {
    try self.realmInitializer()
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier = %@", id)
  }

  func get(by id: String) throws -> PhDownloadTaskEntity? {
    Self.find(by: id, in: try self.realmInitializer())
  }

  private static func find(by id: String, in realm: RealmAdapter) -> PhDownloadTaskEntity? {
    realm.object(ofType: PhDownloadTaskEntity.self, forPrimaryKey: id)
  }
}

class PhDownloadTaskEntity: Object {
  @objc dynamic var identifier: String = ""
  @objc dynamic var url: String = ""
  @objc dynamic var fileName: String = ""
  @objc dynamic var savedDir: String = ""
  @objc dynamic var state: Int = undefined
  @objc dynamic var updatedAt: Date = .init()

  override class func primaryKey() -> String? { "identifier" }

  // MARK: Integer constants as download state

  static let undefined = -2
  static let enqueued = -1
  static let completed = 101
  static let failed = 102
  static let cancelled = 103

  convenience init(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: Int
  ) {
    self.init()
    self.identifier = identifier
    self.url = url.absoluteString
    self.fileName = fileName
    self.savedDir = savedDir.path
    self.state = state
  }
}

extension PhDownloadState {
  var toInt: Int {
    switch self {
    case .undefined:
      return PhDownloadTaskEntity.undefined
    case .enqueued:
      return PhDownloadTaskEntity.enqueued
    case .downloading(let progress):
      return progress
    case .completed:
      return PhDownloadTaskEntity.completed
    case .failed:
      return PhDownloadTaskEntity.failed
    case .cancelled:
      return PhDownloadTaskEntity.cancelled
    }
  }
}

extension Int {
  var toDownloadState: PhDownloadState {
    switch self {
    case PhDownloadTaskEntity.undefined:
      return .undefined
    case PhDownloadTaskEntity.enqueued:
      return .enqueued
    case 0...100:
      return .downloading(progress: self)
    case PhDownloadTaskEntity.completed:
      return .completed
    case PhDownloadTaskEntity.failed:
      return .failed
    case PhDownloadTaskEntity.cancelled:
      return .cancelled
    default:
      fatalError("Invalid state: \(self)")
    }
  }
}

extension RxProgress {
  var asDownloadState: PhDownloadState {
    if self.totalBytes <= 0 { return .downloading(progress: 0) }
    let percent = Double(self.bytesWritten) / Double(self.totalBytes)
    return .downloading(progress: Int(100 * percent))
  }
}

extension Event where Element == RxProgress {
  func asDownloadState(_ isCompleted: Bool) -> PhDownloadState {
    switch self {
    case .next(let progress):
      return progress.asDownloadState
    case .error:
      return .failed
    case .completed:
      return isCompleted ? .completed : .cancelled
    }
  }
}

/// The command that enqueues a download request or cancel by identifier
enum Command {
  case enqueue(request: PhDownloadRequest)
  case cancel(identifier: String)
}

// MARK: - RealSwiftyDownloader

final class RealSwiftyDownloader: PhDownloader {
  // MARK: Dependencies
  private let options: PhDownloaderOptions
  private let dataSource: PhDownloadLocalDataSource

  // MARK: ReactiveX
  private let commandS = PublishRelay<Command>()
  private let downloadResultS = PublishRelay<PhDownloadResult>()
  private let disposeBag = DisposeBag()

  // MARK: Schedulers
  private let throttleScheduler = SerialDispatchQueueScheduler(qos: .utility)
  private static var mainScheduler: MainScheduler { .instance }
  private static var concurrentMainScheduler: ConcurrentMainScheduler { .instance }

  internal init(options: PhDownloaderOptions, dataSource: PhDownloadLocalDataSource) {
    self.options = options
    self.dataSource = dataSource

    self
      .commandS
      .compactMap {
        if case .enqueue(request: let request) = $0 { return request }
        return nil
      }
      .map { [weak self] request in self?.createDownloadRequest(request) ?? .empty() }
      .merge(maxConcurrent: options.maxConcurrent)
      .subscribe()
      .disposed(by: self.disposeBag)
  }

  // MARK: Private helpers

  private func createDownloadRequest(_ request: PhDownloadRequest) -> Observable<Void> {
    var isCompleted = false

    return Observable
      .deferred { [dataSource] () -> Observable<DownloadRequest> in
        // already cancelled before
        if (try dataSource.get(by: request.identifier))?.state.toDownloadState == .cancelled {
          return .empty()
        }

        let urlRequest = URLRequest(url: request.url)
        let destination: DownloadRequest.Destination = { (temporaryURL, response) in
          (
            request.savedDir.appendingPathComponent(request.fileName),
            [.createIntermediateDirectories, .removePreviousFile]
          )
        }
        return RxAlamofire.download(urlRequest, to: destination)
      }
      .subscribeOn(Self.concurrentMainScheduler)
      .flatMap { $0.rx.progress() }
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { isCompleted = true })
      .takeUntil(self.cancelCommand(for: request.identifier))
      .throttle(self.options.throttleProgress, latest: true, scheduler: self.throttleScheduler)
      .distinctUntilChanged()
      .materialize()
      .observeOn(Self.mainScheduler)
      .do(
        onNext: { [downloadResultS] notification in
          switch notification {
          case .error(let error):
            downloadResultS.accept(.failure(.downloadError(error)))
          case .completed:
            downloadResultS.accept(.success(request))
          case .next:
            ()
          }
        }
      )
      .flatMap { [dataSource] notification -> Completable in
        dataSource.update(
          id: request.identifier,
          state: notification.asDownloadState(isCompleted)
        )
      }
      .map { _ in () }
      .catchError { error in
        print("Download error: \(error)")
        return .empty()
      }
  }

  private func cancelCommand(for identifierNeedCancel: String) -> Observable<Void> {
    self.commandS.compactMap {
      if case .cancel(let identifier) = $0, identifier == identifierNeedCancel { return () }
      return nil
    }
  }

  private func cancelDownload(_ identifier: String) -> Completable {
    return .create { observer -> Disposable in
      let disposable = BooleanDisposable()
      let compositeDisposable = CompositeDisposable()
      _ = compositeDisposable.insert(disposable)

      DispatchQueue.main.async {
        do {
          if disposable.isDisposed { return }

          guard let task = try self.dataSource.get(by: identifier) else {
            return observer(.error(PhDownloaderError.notFound(identifier: identifier)))
          }

          if disposable.isDisposed { return }

          let fileURL = URL(fileURLWithPath: task.savedDir).appendingPathComponent(task.fileName)
          try? FileManager.default.removeItem(at: fileURL)

          if disposable.isDisposed { return }

          _ = compositeDisposable.insert(
            self.dataSource
              .update(id: identifier, state: .cancelled)
              .subscribe(observer)
          )
        } catch {
          observer(.error(PhDownloaderError.databaseError(error)))
        }
      }

      return compositeDisposable
    }
  }
}

// MARK: Observe state by identifier
extension RealSwiftyDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState> {
    Observable
      .deferred { [dataSource] () -> Observable<PhDownloadState> in
        do {
          if let task = try dataSource.get(by: identifier) {
            return Observable
              .from(object: task, emitInitialValue: true)
              .map { $0.state.toDownloadState }
              .subscribeOn(Self.concurrentMainScheduler)
          }

          return Observable
            .collection(
              from: try dataSource.getResults(by: identifier),
              synchronousStart: true,
              on: .main
            )
            .map { results in results.first?.state.toDownloadState ?? .undefined }
            .subscribeOn(Self.concurrentMainScheduler)

        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
      }
      .distinctUntilChanged()
  }

}

// MARK: Observe state by identifiers
extension RealSwiftyDownloader {
  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String {
      .deferred { [dataSource] () -> Observable<[String : PhDownloadState]> in
        do {
          return Observable
            .collection(
              from: try dataSource.getResults(by: Set(identifiers)),
              synchronousStart: true,
              on: .main
            )
            .subscribeOn(Self.concurrentMainScheduler)
            .map { results in
              let tuples = Array(results).map { ($0.identifier, $0.state.toDownloadState) }
              let stateById = Dictionary(uniqueKeysWithValues: tuples)
              return Dictionary(uniqueKeysWithValues: identifiers.map { ($0, stateById[$0] ?? .undefined) })
            }
            .distinctUntilChanged()
        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
    }
  }
}

// MARK: Download result observable
extension RealSwiftyDownloader {
  var downloadResult$: Observable<PhDownloadResult> { self.downloadResultS.asObservable() }
}

// MARK: Enqueue download request
extension RealSwiftyDownloader {
  func enqueue(_ request: PhDownloadRequest) -> Completable {
    // insert or update task into database
    // and then, send command to enqueue download request
    self.dataSource
      .insertOrUpdate(
        identifier: request.identifier,
        url: request.url,
        fileName: request.fileName,
        savedDir: request.savedDir,
        state: .enqueued
      )
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { [commandS] in commandS.accept(.enqueue(request: request)) })
  }
}

// MARK: Cancel by identifier
extension RealSwiftyDownloader {
  func cancel(by identifier: String) -> Completable {
    // mask task as cancelled to prevent executing enqueued task
    // and then, send command to cancel downloading task
    self.cancelDownload(identifier)
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { [commandS] in commandS.accept(.cancel(identifier: identifier)) })
  }
}

// MARK: Cancel all
extension RealSwiftyDownloader {
  func cancelAll() -> Completable {
    fatalError()
  }
}
